{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Lens ( (^..) )
import Control.Monad.Reader (runReaderT, ask, ReaderT)
import Control.Monad.Trans (lift)
import Data.Aeson ( Value(..) )
import Data.Aeson.Lens (key, _Bool, _String, _Array, values)
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

processTable x = lift $ do
  putStr "WARNING: unknown table definition synax: "
  print x

exportSql :: T.Text -> ReaderT Environment IO ()
exportSql s = do
   h <- _exportHandle <$> ask
   lift $ hPutStrLn h (T.unpack s)

importSql :: T.Text -> ReaderT Environment IO ()
importSql s = do
   h <- _importHandle <$> ask
   lift $ hPutStrLn h (T.unpack s)

