{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecursiveDo #-}

module Main where

import Prelude hiding (lookup)

import Control.Lens ( (^..) )
import Control.Monad.Reader (runReaderT, ask, ReaderT)
import Control.Monad.IO.Class (liftIO, MonadIO)
import Data.Aeson ( Value(..) )
import Data.Aeson.Lens (key, _Bool, _String, _Array, values)
import qualified Data.ByteString.Char8 as BS8
import Data.Function (on)
import Data.HashMap.Strict (HashMap(..), elems, keys, lookup)
import Data.List (nub, groupBy, intersperse, sortBy, find)
import Data.Maybe (fromMaybe)
import Data.Monoid ( (<>) )
import qualified Data.Text as T
import Data.Traversable (for)
import qualified Data.Vector as V
import Data.Yaml
import Database.PostgreSQL.Simple as PSQL
import Options.Applicative (strOption, long, metavar, help,
                            execParser, info, helper, fullDesc, progDesc, header)
import qualified Options.Applicative as Optparse
import System.IO (Handle, openFile, IOMode(..), hClose, hPutStrLn)
import Lib

data CommandLine = CommandLine
  {
    _dataDir :: T.Text
  , _tablesFilename :: String
  , _connStr :: String
  }

commandLineOptions :: Optparse.Parser CommandLine
commandLineOptions = CommandLine
  <$> (T.pack <$> (strOption ( long "directory"
                <> metavar "DIRECTORY"
                <> help "Directory for dump files"
                )))
  <*> strOption ( long "tables"
                <> metavar "YAML-FILE"
                <> help "Table definitions file"
                )
  <*> strOption ( long "db"
                <> metavar "CONNECT-STRING"
                <> help "postgres connection string"
                )

data Environment = Environment
  { _exportHandle :: Handle
  , _importHandle :: Handle
  , _commandline :: CommandLine
  , _databaseConnection :: PSQL.Connection
  }

getTablesFilename = (_tablesFilename . _commandline) <$> ask
getDataDir = (_dataDir . _commandline) <$> ask

data TableSpec = TableSpec
  { _tableName :: T.Text,
    _shrink :: Maybe T.Text,
    _requires :: [T.Text]
  } deriving Show

main :: IO ()
main = do
  progress "postgres-subset"

  withEnvironment $ do

    importSql "\\set ON_ERROR_STOP on"

    -- get table specificatiosn from database
    -- this will include, perhaps, some tables that are not
    -- mentioned in the table file, and should be
    -- the definitive list of tables, rather than the
    -- list in the table defs file (which should instead
    -- be annotations on this)
    -- In addition to a list of table names, foreign
    -- key constraints should be read to generate
    -- `requires` entries.


    -- this 'rec' block allows access to the final tableDefs
    -- value in the code that is constructing it.
    rec  
      conn <- _databaseConnection <$> ask
      dbTableDefs <- liftIO $ query_ conn "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
      liftIO $ print (dbTableDefs :: [[String]])

      dbTables <- for dbTableDefs $ \[tableName] -> do
        progress $ "Table in database: " ++ tableName

      -- courtesy of http://stackoverflow.com/questions/1152260/postgres-sql-to-list-table-foreign-keys
        fkeys <- liftIO $ query conn "SELECT \
    \   kcu.column_name, \
    \   ccu.table_name AS foreign_table_name, \
    \   ccu.column_name AS foreign_column_name  \
    \ FROM \
    \  information_schema.table_constraints AS tc \
    \  JOIN information_schema.key_column_usage AS kcu \
    \  ON tc.constraint_name = kcu.constraint_name \
    \  JOIN information_schema.constraint_column_usage AS ccu \
    \  ON ccu.constraint_name = tc.constraint_name \
    \ WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name=?" 
           [tableName]
        liftIO $ putStr "Foreign key definitions: "
        liftIO $ print (fkeys :: [[String]])
        let fkeyTables = map (T.pack . (!!1) ) fkeys
        liftIO $ putStr "Foreign key tables: "
        liftIO $ print (fkeyTables)

      -- if any of our 'requires' tables have been
      -- shrunk, we need to shrink this table
      -- accordingly, so that the foreign keys
      -- are not violated.

      -- but at the moment, we don't know if the
      -- table has been shrunk or not! because we
      -- can also shrink in the config file.

      -- and this needs to happen recursively.
      -- which is a bit awkward because we're living in
      -- ReaderT IO here (although only for the
      -- purposes of output tracing...)

        requiresTables <- for fkeys $ \fk -> do
          let foreignTableName = T.pack $ fk !! 1
          if (fk !! 1) == tableName
            then return Nothing
            else do
              -- this must exist, because all tables are in tableDefs
              let (Just tableSpec) =  find (\table -> _tableName table == foreignTableName) tableDefs
              return $ case _shrink tableSpec of
                Nothing -> Nothing
                Just _ -> Just $ T.pack $
                     "("
                  ++     "(" ++ (fk !! 0) ++ " IN (SELECT " ++ (fk !! 2) ++ " FROM " ++ (fk !! 1) ++ ") )" 
                  ++ " OR ( " ++ fk !! 0 ++ " IS NULL)"  -- TODO: do this only when the fk is nullable, because
                                                         -- it makes the query substantially more expensive
                  ++ ")"
                -- ^ we don't care how the foreign table was shrunk,
                --   just whether it was shrunk or not.

        let fkeyShrink = foldr sqlAnd Nothing requiresTables

        return $ TableSpec {
            _tableName = T.pack tableName
          , _shrink = fkeyShrink
          , _requires = fkeyTables
          }

      tablesFilename <- getTablesFilename
      tablesYaml :: Value <- fromMaybe
                               (error "Cannot parse tables file")
                            <$> (liftIO . decodeFile) tablesFilename

      -- get table specifications from file
      let configTableDefs = tablesYaml ^.. key "tables" . values
      configTableDefs' <- for configTableDefs parseTable

      let tableDefs = mergeTableDefs dbTables configTableDefs'

    liftIO $ putStrLn "TABLE DEFS:"
    liftIO $ print tableDefs

    tableDefs'' <- orderByRequires tableDefs

    for tableDefs'' processTable

  progress "done."

mergeTableDefs :: [TableSpec] -> [TableSpec] -> [TableSpec]
mergeTableDefs t1 t2 = map mergeTable groups
  where groups = groupBy eqName (sortName $ t1 ++ t2)
        eqName = (==) `on` _tableName
        compareName = compare `on` _tableName
        sortName t = sortBy compareName t
        mergeTable [l] = l -- express this as a fold, more generally?
        mergeTable [l,r] = TableSpec {
            _tableName = _tableName l -- assert == _tableName r
          , _shrink = (_shrink l) `sqlAnd` (_shrink r)
          , _requires = nub $ (_requires l) ++ (_requires r)
          }

sqlAnd :: Maybe T.Text -> Maybe T.Text -> Maybe T.Text
sqlAnd Nothing r = r
sqlAnd l Nothing = l
sqlAnd (Just l) (Just r) = Just $ "(" <> l <> ") AND (" <> r <> ")"

withEnvironment :: ReaderT Environment IO a -> IO a
withEnvironment a = do
  cli <- execParser $ info (helper <*> commandLineOptions)
                 ( fullDesc
                <> progDesc "Generate subsets of a database"
                <> header "postgres-subset - a database subset helper"
                 )
  exportHandle <- openFile (T.unpack (_dataDir cli) <> "/export.sql") WriteMode
  importHandle <- openFile (T.unpack (_dataDir cli) <> "/import.sql") WriteMode

  conn <- PSQL.connectPostgreSQL (BS8.pack $ _connStr cli)

  let env = Environment
       { _exportHandle = exportHandle 
       , _importHandle = importHandle
       , _commandline = cli
       , _databaseConnection = conn
       }
  v <- runReaderT a env

  close conn -- beware of laziness in processing table results here - should do at end?
  hClose exportHandle
  hClose importHandle
  return v

progress :: MonadIO m => String -> m ()
progress = liftIO . putStrLn

parseTable :: Value -> ReaderT Environment IO TableSpec
parseTable (String tableName) = return $ TableSpec
  { _tableName = tableName
  , _shrink = Nothing
  , _requires = []
  }

parseTable (Object (l :: HashMap T.Text Value)) = do
  let tableName = (head $ keys l)
  let (Object def) = (head $ elems l)

  shrinkSql <- case lookup "shrink" def of
    Nothing -> do liftIO $ putStrLn "Not shrinking this table"
                  return Nothing
    (Just (String s)) -> do
      liftIO $ putStrLn $ "Shrink SQL: " <> show s
      return $ Just s

    Just x -> error $ "Unknown shrink syntax: " <> show x

  let requiresValue = lookup "requires" def
  liftIO $ putStrLn $ "Requires: " <> show requiresValue
  let requires = case requiresValue of 
        Just (String singleRequirement) -> [singleRequirement]
        Just (Array strings) -> map (\(String s) -> s) (V.toList strings)
        Nothing -> []
        x -> error $ "Unknown requires: case: " <> show x

  return $ TableSpec
    { _tableName = tableName
    , _shrink = shrinkSql
    , _requires = requires
    }

parseTable x = error $ "WARNING: unknown table definition synax: " <> show x

processTable tspec = do

  let tableName = _tableName tspec
  progress $ "Processing table definition: " <> (show $ _tableName tspec)
  exportNote $ "Table: " <> tableName
  exportNote $ "Requires: " <> (foldr (<>) (T.pack "") $ intersperse (T.pack ", ") $ _requires tspec)

  case (_shrink tspec) of
    Nothing -> liftIO $ putStrLn "Not shrinking this table"
    (Just shrinkSql) -> do
      exportNote $ "Shrinking: " <> tableName
      liftIO $ putStrLn $ "Shrink SQL: " <> show shrinkSql
      exportShrink $ "CREATE TEMPORARY TABLE " <> tableName <> " AS SELECT * FROM " <> tableName <> " WHERE " <> shrinkSql <> ";"

  liftIO $ putStrLn $ "Requires: " <> show (_requires tspec)
  dumpDir <- getDataDir
  exportNote $ "Exporting: " <> tableName
  importSql $ "\\echo Importing: " <> tableName
  exportTable tableName
  importSql $ "\\copy " <> tableName <> " from '" <> tableName <> ".dump';"
  importSql $ "ANALYZE " <> tableName <> ";"

exportTable :: T.Text -> ReaderT Environment IO ()
exportTable tableName = do
   let sql = "\\copy " <> tableName <> " to '" <> tableName <> ".dump'"
   exportSql sql

exportNote :: T.Text -> ReaderT Environment IO ()
exportNote s = exportSql ("\\echo " <> s)

exportShrink :: T.Text -> ReaderT Environment IO ()
exportShrink s = exportSql s

exportSql :: T.Text -> ReaderT Environment IO ()
exportSql s = do
   h <- _exportHandle <$> ask
   liftIO $ hPutStrLn h (T.unpack s)

importSql :: T.Text -> ReaderT Environment IO ()
importSql s = do
   h <- _importHandle <$> ask
   liftIO $ hPutStrLn h (T.unpack s)


-- | Sorts the supplied tables by the requirements field.
-- Probably loops forever if the requirements do not form a
-- partial order.
orderByRequires :: [TableSpec] -> ReaderT Environment IO [TableSpec]
orderByRequires ts = o [] ts
  where
    o :: [T.Text] -> [TableSpec] -> ReaderT Environment IO [TableSpec]
    o _ [] = return []
    o _ [t] = return [t]
    o done (t:ts) = do

      -- check that all requirements are in the `done` list already.
      let isDone t' = t' `elem` done || t' == (_tableName t)
      let satisfied = not (False `elem` (nub (map isDone (_requires t))))
      let missing = filter (not . isDone) (_requires t)

      -- if requirements are not all done, punt this element to the
      -- end.
      if satisfied
        then do
          liftIO $ putStrLn $ "Dependencies satisfied for table " <> (show $ _tableName t)
          subs <- o ((_tableName t):done) ts
          return $ t : subs
        else (liftIO $ putStrLn $ "Deferring " <> show t <> ": missing tables: " <> show missing) >> (o done (ts ++ [t]))

