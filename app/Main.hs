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
  c "sql import/export generator"
  exportHandle <- openFile "export.sql" WriteMode
  importHandle <- openFile "import.sql" WriteMode
  tablesYaml :: Value <- fromMaybe
                           (error "Cannot parse tables file")
                       <$> decodeFile "tables.yaml"
  c "Tables YAML:"
  let tableDefs = tablesYaml ^.. key "tables" . values

  let env = Environment exportHandle importHandle
  (flip runReaderT) env $ do
    tableDefs' <- for tableDefs parseTable
    tableDefs'' <- lift $ orderByRequires tableDefs'
    for tableDefs'' processTable

  c "done."
  hClose exportHandle
  hClose importHandle

c :: String -> IO ()
c = putStrLn

parseTable :: Value -> ReaderT Environment IO TableSpec
parseTable (String tableName) = return $ TableSpec
  { _tableName = tableName
  , _shrink = Nothing
  , _requires = []
  }

parseTable (Object (l :: HashMap T.Text Value)) = do
  lift $ putStr "Processing map definition: "
  lift $ print $ l
  let tableName = (head $ keys l)
  lift $ print tableName
  let (Object def) = (head $ elems l)
  lift $ print def

  -- TODO: shrink needs to be optional.
  shrinkSql <- case lookup "shrink" def of
    Nothing -> do lift $ putStrLn "Not shrinking this table"
                  return Nothing
    (Just (String s)) -> do
      lift $ putStrLn $ "Shrink SQL: " <> show s
      return $ Just s

--      exportSql $ "CREATE TEMPORARY TABLE " <> tableName <> " AS " <> shrinkSql <> ";"

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
  lift $ putStr "Processing table definition: "
  lift $ print (_tableName tspec)
  case (_shrink tspec) of
    Nothing -> lift $ putStrLn "Not shrinking this table"
    (Just shrinkSql) -> do
      lift $ putStrLn $ "Shrink SQL: " <> show shrinkSql
      exportSql $ "CREATE TEMPORARY TABLE " <> tableName <> " AS " <> shrinkSql <> ";"

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
orderByRequires :: [TableSpec] -> IO [TableSpec]
orderByRequires ts = internalOrderByRequires [] [] ts
  where
    internalOrderByRequires :: [T.Text] -> [T.Text] -> [TableSpec] -> IO [TableSpec]
    internalOrderByRequires _ _ [] = return []
    internalOrderByRequires _ _ [x] = return [x]
                                        -- don't check the requirements on 
                                        -- the final case, because we 
                                        -- assume that the partial order is
                                        --  actually a partial order.
    internalOrderByRequires done inprogress (x:xs) = do
      -- Are all the requirements done already? If so, output x.
      -- Otherwise, punt x to the end, and recurse without shortening the
      -- list, so that in the case of an inconsistent partial order, we'll
      -- loop forever.
      let kl = (nub (map (\x -> x `elem` done) (_requires x)))
      putStrLn $ "kl = " <> show kl
      if not (False `elem` kl)
        then do
          putStrLn "SHRINK"
          subs <- internalOrderByRequires ((_tableName x):done) inprogress xs
          return $ x : subs
        else if False && (_tableName x) `elem` inprogress then error ("LOOP: " <> (show $ _tableName x) <> ", in progress = " <> show inprogress <> ", done = " <> show done) else (putStrLn $ "PERMUTE " <> show x <> ", done = " <> show done <> ", inprogress = " <> show inprogress) >> (internalOrderByRequires done ((_tableName x):inprogress) (xs ++ [x]))

