{-# language LambdaCase #-}
{-# language NamedFieldPuns #-}
{-# language OverloadedStrings #-}

import Control.Monad (when)
import Data.Bytes (Bytes)
import Data.Foldable (for_)
import Data.Primitive (Array)
import Cauldron (Cauldron(..),Metadata(..),Compression(..),Messages(..))

import qualified Data.Bytes as Bytes
import qualified Data.Bytes.Builder as Builder
import qualified Data.Bytes.Chunks as Chunks
import qualified Data.Primitive as PM
import qualified Cauldron as Cauldron
import qualified GHC.Exts as Exts
import qualified Json

main :: IO ()
main = do
  putStrLn "Test A"
  case roundtrip (Cauldron.plaintext None metaA rawsA) of
    Nothing -> fail "Failed to decode"
    Just (Cauldron{metadata,units})
      | metaA /= metadata -> fail "Metadata did not roundtrip"
      | Cauldron.messages_ units /= Plaintext rawsA ->
          fail "Units did not roundtrip"
      | otherwise -> pure ()
  putStrLn "Test B"
  case roundtrip (Cauldron.json None metaA rawsB) of
    Nothing -> fail "Failed to decode"
    Just (Cauldron{metadata,units})
      | metaA /= metadata -> fail "Metadata did not roundtrip"
      | Cauldron.messages_ units /= Json rawsB ->
          fail "Units did not roundtrip"
      | otherwise -> pure ()
  putStrLn "Test C"
  case roundtrip (Cauldron.plaintext Lz4 metaA rawsA) of
    Nothing -> fail "Failed to decode"
    Just (Cauldron{metadata,units})
      | metaA /= metadata -> fail "Metadata did not roundtrip"
      | Cauldron.messages_ units /= Plaintext rawsA ->
          fail "Units did not roundtrip"
      | otherwise -> pure ()
  putStrLn "Test D"
  case roundtrip (Cauldron.json Lz4 metaA rawsB) of
    Nothing -> fail "Failed to decode"
    Just (Cauldron{metadata,units})
      | metaA /= metadata -> fail "Metadata did not roundtrip"
      | Cauldron.messages_ units /= Json rawsB ->
          fail "Units did not roundtrip"
      | otherwise -> pure ()
  putStrLn "Finished"

roundtrip :: Cauldron -> Maybe Cauldron
roundtrip =
  Cauldron.decode . Chunks.concat . Builder.run 20 . Cauldron.encode

metaA :: Metadata
metaA = Metadata
  { customer = 5
  , schema = 7
  , uuid = 0xFFFF
  , timestamps = PM.replicatePrimArray 2 324626426
  }

rawsA :: Array Bytes
rawsA = Exts.fromList
  [ Bytes.fromLatinString "hello world"
  , Bytes.fromLatinString "there is more data"
  ]

rawsB :: Array Json.Value
rawsB = Exts.fromList
  [ Json.String "hey"
  , Json.False 
  ]
