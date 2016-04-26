{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Prelude hiding (lookup)

import Control.Lens ( (^..) )
import Control.Monad.Reader (runReaderT, ask, ReaderT)
import Control.Monad.Trans (lift)
import Data.Aeson ( Value(..) )
import Data.Aeson.Lens (key, _Bool, _String, _Array, values)
import Data.HashMap.Strict (HashMap(..), elems, keys, lookup)
import Data.List (nub)
import Data.Maybe (fromMaybe)
import Data.Monoid ( (<>) )
import qualified Data.Text as T
import Data.Traversable (for)
import qualified Data.Vector as V
import Data.Yaml
import System.IO (Handle, openFile, IOMode(..), hClose, hPutStrLn)
import Lib

tablesFilename = "tables.yaml"
dumpDir = "/home/benc/tmp/smalldump/"

data Environment = Environment
  { _exportHandle :: Handle
  , _importHandle :: Handle
  }


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

    tablesYaml :: Value <- fromMaybe
                             (error "Cannot parse tables file")
                          <$> (lift . decodeFile) tablesFilename
    let tableDefs = tablesYaml ^.. key "tables" . values
    tableDefs' <- for tableDefs parseTable
    tableDefs'' <- orderByRequires tableDefs'
    for tableDefs'' processTable

  progress "done."

withEnvironment :: ReaderT Environment IO a -> IO a
withEnvironment a = do
  exportHandle <- openFile "export.sql" WriteMode
  importHandle <- openFile "import.sql" WriteMode
  let env = Environment exportHandle importHandle
  v <- runReaderT a env
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
  case (_shrink tspec) of
    Nothing -> lift $ putStrLn "Not shrinking this table"
    (Just shrinkSql) -> do
      lift $ putStrLn $ "Shrink SQL: " <> show shrinkSql
      exportSql $ "CREATE TEMPORARY TABLE " <> tableName <> " AS SELECT * FROM " <> tableName <> " WHERE " <> shrinkSql <> ";"

  lift $ putStrLn $ "Requires: " <> show (_requires tspec)
  exportSql $ "COPY " <> tableName <> " TO '" <> dumpDir <> "/" <> tableName <> ".dump';"
  importSql $ "COPY " <> tableName <> " FROM '" <> dumpDir <> "/" <> tableName <> ".dump';"
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
      let isDone t' = t' `elem` done
      let satisfied = not (False `elem` (nub (map isDone (_requires t))))

      -- if requirements are not all done, punt this element to the
      -- end.
      if satisfied
        then do
          lift $ putStrLn $ "Dependencies satisfied for table " <> (show $ _tableName t)
          subs <- o ((_tableName t):done) ts
          return $ t : subs
        else (lift $ putStrLn $ "Deferring " <> show t <> ", done = " <> show done) >> (o done (ts ++ [t]))

