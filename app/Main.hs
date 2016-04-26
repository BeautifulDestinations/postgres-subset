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
import Data.Maybe (fromMaybe)
import Data.Monoid ( (<>) )
import qualified Data.Text as T
import Data.Traversable (for)
import Data.Yaml
import System.IO (Handle, openFile, IOMode(..), hClose, hPutStrLn)
import Lib

dumpDir = "/home/benc/tmp/smalldump/"

data Environment = Environment
  { _exportHandle :: Handle
  , _importHandle :: Handle
  }

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
  for tableDefs (\v -> runReaderT (processTable v) env)

  c "done."
  hClose exportHandle
  hClose importHandle

c :: String -> IO ()
c = putStrLn

processTable (String tableName) = do
  lift $ putStr "Processing simple definition: "
  lift $ print tableName
  exportSql $ "COPY " <> tableName <> " TO '" <> dumpDir <> "/" <> tableName <> ".dump';"
  importSql $ "COPY " <> tableName <> " FROM '" <> dumpDir <> "/" <> tableName <> ".dump';"

-- Object (fromList [("accounts",Object (fromList [("shrink",String "select * from accounts where tier = 1 union select * from accounts where id in (select followed_account_id from followers where follower_id = 225239379) union select * from accounts where id in (select follower_id from followers where followed_account_id = 225239379)"),("requires",String "sectors")]))])

processTable (Object (l :: HashMap T.Text Value)) = do
  lift $ putStr "Processing map definition: "
  -- assert l is an object with one entry
  lift $ print $ l
  let tableName = (head $ keys l)
  lift $ print tableName
  let (Object def) = (head $ elems l)
  lift $ print def
  let (Just (String shrink)) = lookup "shrink" def
  let requires = lookup "requires" def
  lift $ do
    putStr "Shrink: "
    print shrink
    putStr "Requires: "
    print requires
  exportSql $ "CREATE TEMPORARY TABLE " <> tableName <> " AS " <> shrink <> ";"
  exportSql $ "COPY " <> tableName <> " TO '" <> dumpDir <> "/" <> tableName <> ".dump';"
  importSql $ "COPY " <> tableName <> " FROM '" <> dumpDir <> "/" <> tableName <> ".dump';"

processTable x = error $ "WARNING: unknown table definition synax: " <> show x

exportSql :: T.Text -> ReaderT Environment IO ()
exportSql s = do
   h <- _exportHandle <$> ask
   lift $ hPutStrLn h (T.unpack s)

importSql :: T.Text -> ReaderT Environment IO ()
importSql s = do
   h <- _importHandle <$> ask
   lift $ hPutStrLn h (T.unpack s)

