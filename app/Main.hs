{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecursiveDo         #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Prelude                                hiding (lookup)

import           Control.Lens                           ((^..))
import           Control.Monad.IO.Class                 (MonadIO, liftIO)
import           Control.Monad.Reader                   (ReaderT, ask,
                                                         runReaderT)
import           Control.Monad.State
import           Data.Aeson                             (Value (..))
import           Data.Aeson.Lens                        (key, values, _Array,
                                                         _Bool, _String)
import qualified Data.ByteString                        as BS
import qualified Data.ByteString.Char8                  as BS8
import           Data.Function                          (on)
import           Data.HashMap.Strict                    (HashMap (..), elems,
                                                         keys, lookup)
import           Data.Int                               (Int64)
import           Data.List                              (find, groupBy,
                                                         intersperse, nub,
                                                         sortBy)
import           Data.Maybe                             (fromMaybe, isJust)
import           Data.Monoid                            ((<>))
import           Data.String                            (fromString)
import qualified Data.Text                              as T
import           Data.Traversable                       (for)
import qualified Data.Vector                            as V
import           Data.Yaml
import           Database.PostgreSQL.Simple             as PSQL
import           Database.PostgreSQL.Simple.Copy        as PSQLC
import           Database.PostgreSQL.Simple.Transaction as PSQLT
import           Lib
import           Options.Applicative                    (execParser, fullDesc,
                                                         header, help, helper,
                                                         info, long, metavar,
                                                         progDesc, strOption)
import qualified Options.Applicative                    as Optparse
import           System.IO                              (Handle, IOMode (..),
                                                         hClose, hPutStrLn,
                                                         openFile)

data CommandLine = CommandLine
  {
    _dataDir        :: T.Text
  , _tablesFilename :: String
  , _connStr        :: String
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
  { _importHandle       :: Handle
  , _commandline        :: CommandLine
  , _databaseConnection :: PSQL.Connection
  }

getTablesFilename = (_tablesFilename . _commandline) <$> ask
getDataDir = (_dataDir . _commandline) <$> ask

data TableSpec = TableSpec
  { _tableName :: T.Text,
    _shrink    :: Maybe T.Text,
    _requires  :: [T.Text]
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
      dbTableDefs <- liftIO $ query_ conn "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_type = 'BASE TABLE' "
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
          -- we should shrink when an upstream table is
          -- shrunk, or if we are referencing ourselves
          -- although in that second case we really only
          -- need to shrink if there is also an
          -- upstream shrink(?)   [PERFORMANCE]
          -- when working on this code, be careful about
          -- laziness: the tableSpec and its constituent
          -- fields all become available at different times
          -- during the run, and <<loops>> can easily
          -- occur.
          let (Just tableSpec) = find (\table -> _tableName table == foreignTableName) tableDefs
          let shouldShrink = (fk !! 1) == tableName || isJust (_shrink tableSpec)
          return $ if shouldShrink
            then Just $ T.pack $
                     "("
                  ++     "(" ++ (fk !! 0) ++ " IN (SELECT " ++ (fk !! 2) ++ " FROM " ++ (fk !! 1) ++ ") )"
                  ++ " OR ( " ++ fk !! 0 ++ " IS NULL)"  -- TODO: do this only when the fk is nullable, because
                                                         -- it makes the query substantially more expensive
                                                         -- [PERFORMANCE]
                  ++ ")"
            else Nothing

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

type AppMonad = StateT Int (ReaderT Environment IO)

getUnique :: AppMonad Int
getUnique = do
  n <- get
  let new = n+1
  put new
  return new

withEnvironment :: AppMonad a -> IO a
withEnvironment a = do
  cli <- execParser $ info (helper <*> commandLineOptions)
                 ( fullDesc
                <> progDesc "Generate subsets of a database"
                <> header "postgres-subset - a database subset helper"
                 )
  importHandle <- openFile (T.unpack (_dataDir cli) <> "/import.sql") WriteMode

  conn <- PSQL.connectPostgreSQL (BS8.pack $ _connStr cli)

  let env = Environment
       { _importHandle = importHandle
       , _commandline = cli
       , _databaseConnection = conn
       }
  v <- withConsistentTransaction conn $ runReaderT (evalStateT a 0) env

  close conn -- beware of laziness in processing table results here - should do at end?
  hClose importHandle
  return v

-- | Runs a transaction with a consistent view of the
--   database so that we don't (for example) generate two
--   table dumps that violate foreign keys with respect
--   to each other.
withConsistentTransaction = PSQLT.withTransactionMode mode
  where mode = PSQLT.TransactionMode PSQLT.RepeatableRead PSQLT.ReadWrite

progress :: MonadIO m => String -> m ()
progress = liftIO . putStrLn

parseTable :: Value -> AppMonad TableSpec
parseTable (String tableName) = return $ TableSpec
  { _tableName = tableName
  , _shrink = Nothing
  , _requires = []
  }

parseTable (Object (l :: HashMap T.Text Value)) = do
  let tableName = (head $ keys l)
  let (Object def) = (head $ elems l)

  shrinkSql <- case lookup "shrink" def of
    Nothing -> do
      liftIO $ putStrLn "Aplying default shrink to this table: random() > 0.9"
      return $ Just "random() > 0.9"
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

  t' <- case (_shrink tspec) of
    Nothing -> do
      liftIO $ putStrLn "Not shrinking this table"
      return tableName
    (Just shrinkSql) -> do
      exportNote $ "Shrinking: " <> tableName

      -- really, this looping shrink behaviour only needs to happen
      -- in the case of loops in the key graph. aside from the cost,
      -- it is harmless to always do it.

      conn <- _databaseConnection <$> ask
      ([[nb]] :: [[Int64]]) <- liftIO $ query_ conn (fromString $ T.unpack $ "SELECT COUNT(*) FROM " <> tableName)
      progress $ "Before shrink, there are " <> show nb <> " rows"

      liftIO $ putStrLn $ "Shrink SQL: " <> show shrinkSql

      -- recursion body will replace the current version of tableName
      -- with a shrunk version, repeatedly until a fixed point is
      -- reached.
      let
       recursion_body inputIsTemp numRowsBefore = do
        liftIO $ putStrLn "Shrink iteration:"
        tempTableName <- newTempTableName
        let shrinkStatement = "CREATE TEMPORARY TABLE " <> tempTableName <> " AS SELECT * FROM " <> tableName <> " WHERE " <> shrinkSql
        numRowsAfter <- exportShrink shrinkStatement


        when inputIsTemp $ do
            -- Table will be renamed to this rather than explicitly deleted,
            -- relying on postgres session handling to delete at the end
            -- of the session. This is so we don't do any explicit DELETEs
            -- for safety.
            tempTableName'' <- newTempTableName
            -- get rid of old temp table to be garbage
            -- do this if the input table is a temporary table
            void $ exportShrink $ "ALTER TABLE pg_temp."<> tableName <> " RENAME TO " <> tempTableName''


        exportShrink $ "ALTER TABLE pg_temp."<> tempTableName <> " RENAME TO " <> tableName
        progress $ "After shrink, there are " <> show numRowsAfter <> " rows"

        case numRowsBefore `compare` numRowsAfter of
          LT -> error "Number of rows increased in shrink - should be impossible"
          EQ -> return tableName
          GT -> recursion_body True numRowsAfter

      recursion_body False nb

  liftIO $ putStrLn $ "Requires: " <> show (_requires tspec)
  dumpDir <- getDataDir
  exportNote $ "Exporting: " <> tableName
  importSql $ "\\echo Importing: " <> tableName
  exportTable tableName t'
  importSql $ "\\copy " <> tableName <> " from '" <> tableName <> ".dump';"
  importSql $ "ANALYZE " <> tableName <> ";"

newTempTableName =  (("postgressubsettemp" <>) . T.pack . show) <$> getUnique

exportTable :: T.Text -> T.Text -> AppMonad ()
exportTable tableName srcTableName = do
   let sql = "COPY " <> (fromString $ T.unpack srcTableName) <> " TO STDOUT"
   conn <- _databaseConnection <$> ask
   cli <- _commandline <$> ask
   liftIO $ PSQLC.copy_ conn sql
   h <- liftIO $ openFile ((T.unpack (_dataDir cli <> "/" <> tableName)) <> ".dump") WriteMode
   drainCopy h
   liftIO $ hClose h

drainCopy :: Handle -> AppMonad ()
drainCopy h = do
  conn <- _databaseConnection <$> ask
  cd <- liftIO $ PSQLC.getCopyData conn
  case cd of
    PSQLC.CopyOutRow bs -> (liftIO $ BS.hPut h bs) >> drainCopy h
    PSQLC.CopyOutDone n -> progress ("CopyOutDone with " <> (show n) <> " rows")

exportNote :: T.Text -> AppMonad ()
exportNote note = progress (T.unpack note)

exportShrink :: T.Text -> AppMonad Int64
exportShrink s = do
  progress $ "Running SQL: " <> (T.unpack s)
  conn <- _databaseConnection <$> ask
  liftIO $ execute_ conn (fromString $ T.unpack s)

importSql :: T.Text -> AppMonad ()
importSql s = do
   h <- _importHandle <$> ask
   liftIO $ hPutStrLn h (T.unpack s)


-- | Sorts the supplied tables by the requirements field.
-- Probably loops forever if the requirements do not form a
-- partial order.
orderByRequires :: [TableSpec] -> AppMonad [TableSpec]
orderByRequires ts = o [] ts
  where
    o :: [T.Text] -> [TableSpec] -> AppMonad [TableSpec]
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

