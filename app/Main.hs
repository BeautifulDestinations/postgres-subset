{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Prelude hiding (lookup)

import Control.Lens ( (^..) )
import Control.Monad.Reader (runReaderT, ask, ReaderT)
import Control.Monad.Trans (lift)
import Data.Aeson ( Value(..) )
import Data.Aeson.Lens (key, _Bool, _String, _Array, values)
import qualified Data.ByteString.Char8 as BS8
import Data.Function (on)
import Data.HashMap.Strict (HashMap(..), elems, keys, lookup)
import Data.List (nub, groupBy, intersperse, sortBy)
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


    conn <- _databaseConnection <$> ask
    dbTableDefs <- lift $ query_ conn "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    lift $ print (dbTableDefs :: [[String]])

    dbTables <- for dbTableDefs $ \[tableName] -> do
      lift $ progress $ "Table in database: " ++ tableName

      -- courtesy of http://stackoverflow.com/questions/1152260/postgres-sql-to-list-table-foreign-keys
      fkeys <- lift $ query conn "SELECT \
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
      lift $ putStr "Foreign key definitions: "
      lift $ print (fkeys :: [[String]])
      let fkeyTables = map (T.pack . (!!1) ) fkeys
      lift $ putStr "Foreign key tables: "
      lift $ print (fkeyTables)

      return $ TableSpec {
          _tableName = T.pack tableName
        , _shrink = Nothing -- populate according to requires (and merge with config-specified stuff)
        , _requires = fkeyTables
        }


    tablesFilename <- getTablesFilename
    tablesYaml :: Value <- fromMaybe
                             (error "Cannot parse tables file")
                          <$> (lift . decodeFile) tablesFilename

    -- get table specifications from file
    let configTableDefs = tablesYaml ^.. key "tables" . values
    configTableDefs' <- for configTableDefs parseTable

    let tableDefs = mergeTableDefs dbTables configTableDefs'


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
          , _shrink = (_shrink l) `sqlcat` (_shrink r)
          , _requires = nub $ (_requires l) ++ (_requires r)
          }
        sqlcat :: Maybe T.Text -> Maybe T.Text -> Maybe T.Text
        sqlcat Nothing r = r
        sqlcat l Nothing = l
        sqlcat (Just l) (Just r) = Just $ "(" <> l <> ") AND (" <> r <> ")"

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

  let env = Environment exportHandle importHandle cli conn
  v <- runReaderT a env

  close conn -- beware of laziness in processing table results here - should do at end?
  hClose exportHandle
  hClose importHandle
  return v

progress :: String -> IO ()
progress = putStrLn

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
    Nothing -> do lift $ putStrLn "Not shrinking this table"
                  return Nothing
    (Just (String s)) -> do
      lift $ putStrLn $ "Shrink SQL: " <> show s
      return $ Just s

    Just x -> error $ "Unknown shrink syntax: " <> show x

  let requiresValue = lookup "requires" def
  lift $ putStrLn $ "Requires: " <> show requiresValue
  let requires = case requiresValue of 
        Just (String singleRequirement) -> [singleRequirement]
        Just (Array strings) -> map (\(String s) -> s) (V.toList strings)
        x -> error $ "Unknown requires: case: " <> show x

  return $ TableSpec
    { _tableName = tableName
    , _shrink = shrinkSql
    , _requires = requires
    }

parseTable x = error $ "WARNING: unknown table definition synax: " <> show x

processTable tspec = do

  let tableName = _tableName tspec
  lift $ progress $ "Processing table definition: " <> (show $ _tableName tspec)
  exportSql $ "-- Table: " <> tableName
  exportSql $ "-- Requires: " <> (foldr (<>) (T.pack "") $ intersperse (T.pack ", ") $ _requires tspec)

  case (_shrink tspec) of
    Nothing -> lift $ putStrLn "Not shrinking this table"
    (Just shrinkSql) -> do
      lift $ putStrLn $ "Shrink SQL: " <> show shrinkSql
      exportSql $ "CREATE TEMPORARY TABLE " <> tableName <> " AS SELECT * FROM " <> tableName <> " WHERE " <> shrinkSql <> ";"

  lift $ putStrLn $ "Requires: " <> show (_requires tspec)
  dumpDir <- getDataDir
  exportSql $ "\\copy " <> tableName <> " to '" <> tableName <> ".dump'"
  importSql $ "\\copy " <> tableName <> " from '" <> tableName <> ".dump';"
  importSql $ "ANALYZE " <> tableName <> ";"

exportSql :: T.Text -> ReaderT Environment IO ()
exportSql s = do
   h <- _exportHandle <$> ask
   lift $ hPutStrLn h (T.unpack s)

importSql :: T.Text -> ReaderT Environment IO ()
importSql s = do
   h <- _importHandle <$> ask
   lift $ hPutStrLn h (T.unpack s)


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
          lift $ putStrLn $ "Dependencies satisfied for table " <> (show $ _tableName t)
          subs <- o ((_tableName t):done) ts
          return $ t : subs
        else (lift $ putStrLn $ "Deferring " <> show t <> ": missing tables: " <> show missing) >> (o done (ts ++ [t]))

