{-# language BangPatterns #-}
{-# language MagicHash #-}
{-# language BlockArguments #-}
{-# language DerivingStrategies #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language LambdaCase #-}
{-# language NamedFieldPuns #-}
{-# language OverloadedStrings #-}
{-# language PatternSynonyms #-}
{-# language ScopedTypeVariables #-}
{-# language TypeApplications #-}
{-# language UnboxedSums #-}

module Cauldron
  ( -- * Batch of raw logs
    Cauldron(..)
  , Compression(..)
  , Metadata(..)
  , Messages(..)
  , Units
    -- * Encoding
  , plaintext
  , json
  , encode
  , decode
    -- * Read-only fields
  , Units.presentations_
  , Units.messages_
  ) where

import Control.Monad (when)
import Cauldron.Unsafe (Cauldron(..),Compression(..),Messages(..),Units(..),Metadata(..))
import Data.Bytes.Chunks (Chunks(ChunksNil))
import Data.Bytes.Builder (Builder)
import Data.Int (Int64)
import Data.Word (Word32,Word16,Word8)
import Data.Primitive (PrimArray(..),ByteArray)
import Data.Bytes (Bytes)
import Data.Primitive (Array)
import Control.Monad.ST (ST,runST)
import GHC.Exts (Int(I#))
import Data.Bytes.Parser (Parser,parseBytesMaybe)
import Data.Bytes.Types (Bytes(Bytes))
import qualified Lz4.Block as Lz4
import qualified Data.Primitive.Contiguous as C
import qualified Arithmetic.Nat as Nat
import qualified Cauldron.Units as Units
import qualified Data.Bytes.Builder as Builder
import qualified Data.Bytes.Builder.Bounded as BB
import qualified Data.Bytes.Builder.Unsafe as Unsafe
import qualified Data.Bytes as Bytes
import qualified Data.Bytes.Parser as Parser
import qualified Data.Bytes.Parser.Leb128 as Leb128
import qualified Data.Bytes.Parser.BigEndian as BE
import qualified Data.Bytes.Chunks as Chunks
import qualified Data.Primitive as PM
import qualified Cauldron.Unsafe as CC
import qualified GHC.Exts as Exts
import qualified Json

plaintext :: Compression -> Metadata -> Array Bytes -> Cauldron
plaintext compression m !vals = CC.Cauldron
  { CC.units = Units
    { compression
    , format = 1
    , header = 0
    , sizes
    , messages = Plaintext vals
    , presentations = vals
    , compressed
    , decompressedSize = Bytes.length allTogether
    , offsets = offsetsFromSizes sizes
    }
  , CC.metadata = m
  }
  where
  sizes = C.map' (fromIntegral @Int @Word32 . Bytes.length) vals
  allTogether = concatArrayBytes vals
  compressed = applyCompression compression allTogether

json :: Compression -> Metadata -> Array Json.Value -> Cauldron
json compression m !vals = CC.Cauldron
  { CC.units = Units
    { compression
    , format = 2
    , header = 0
    , sizes
    , messages = Json vals
    , presentations = multislice raw 0 sizes
    , compressed
    , decompressedSize
    , offsets = offsetsFromSizes sizes
    }
  , CC.metadata = m
  }
  where
  EncodingResults raw sizes decompressedSize = encodeJsonMessages vals
  compressed = applyCompression compression (Bytes.fromByteArray raw)

data EncodingResults = EncodingResults
  {-# UNPACK #-} !ByteArray -- concatenated encoded messages (uncompressed)
  {-# UNPACK #-} !(PrimArray Word32) -- sizes
  {-# UNPACK #-} !Int -- total size

offsetsFromSizes :: PrimArray Word32 -> PrimArray Word32
offsetsFromSizes szs = runST do
  let n = PM.sizeofPrimArray szs
  dst <- PM.newPrimArray n
  let go !acc !ix = if ix < n
        then do
          PM.writePrimArray dst ix (fromIntegral acc :: Word32)
          go (acc + fromIntegral @Word32 @Int (PM.indexPrimArray szs ix)) (ix + 1)
        else PM.unsafeFreezePrimArray dst
  go (0 :: Int) 0

encodeJsonMessages :: Array Json.Value -> EncodingResults
encodeJsonMessages !xs = runST do
  let n = PM.sizeofArray xs
  s <- Unsafe.newBuilderState 1008
  lens <- PM.newPrimArray n
  encodeJsonAndComputeLengths xs 0 n lens 0 s

-- It is a pain to run a builder and compute length at the
-- same time. Nevertheless, it can be done.
encodeJsonAndComputeLengths ::
     Array Json.Value
  -> Int -- index into array
  -> Int -- remaining entries
  -> PM.MutablePrimArray s Word32
  -> Int
  -> Unsafe.BuilderState s
  -> ST s EncodingResults
encodeJsonAndComputeLengths !vals !ix !n !lens !totalLen !s0 = case n of
  0 -> do
    lens' <- PM.unsafeFreezePrimArray lens
    cs <- Unsafe.reverseCommitsOntoChunks ChunksNil (Unsafe.closeBuilderState s0)
    let cs' = Chunks.concatU cs
    pure (EncodingResults cs' lens' totalLen)
  _ -> do
    val <- PM.indexArrayM vals ix
    let !(Unsafe.BuilderState buf0 off0 _ _) = s0
    s1@(Unsafe.BuilderState buf1 off1 _ cmts1) <- Unsafe.pasteST (Json.encode val) s0
    let !distance = Unsafe.commitDistance1 buf0 off0 buf1 off1 cmts1
    PM.writePrimArray lens ix (fromIntegral @Int @Word32 (I# distance))
    encodeJsonAndComputeLengths vals (ix + 1) (n - 1) lens (totalLen + I# distance) s1

concatArrayBytes :: Array Bytes -> Bytes
concatArrayBytes = mconcat . Exts.toList

applyCompression :: Compression -> Bytes -> Bytes
applyCompression !cmpr !b = case cmpr of
  None -> b
  Lz4 -> Lz4.compress 1 b

timestampsBuilder :: PrimArray Int64 -> Builder
timestampsBuilder = foldlrPrimArray
  (\_ w -> fromIntegral @Int64 @Int w)
  ( \ !prevLen !curLen bldr ->
    let !deltaLen = fromIntegral @Int64 @Int curLen - prevLen
     in Builder.intLEB128 deltaLen <> bldr
  )
  0
  mempty

foldlrPrimArray :: PM.Prim a
  => (c -> a -> c)
  -> (c -> a -> b -> b)
  -> c
  -> b
  -> PrimArray a
  -> b
{-# INLINE foldlrPrimArray #-}
foldlrPrimArray g f !c0 z !ary = go 0 c0 where
  !sz = PM.sizeofPrimArray ary
  go !i !c = if i == sz
    then z
    else let !x = PM.indexPrimArray ary i in
      f c x (go (i+1) (g c x))

encodeMetadata :: Metadata -> Builder
encodeMetadata Metadata{customer,schema,origin,uuid,timestamps} =
  Builder.fromBounded Nat.constant
    ( BB.wordLEB128 (fromIntegral @Int @Word (PM.sizeofPrimArray timestamps)) `BB.append`
      BB.word64LEB128 customer `BB.append`
      BB.word64LEB128 schema `BB.append`
      BB.word128BE origin `BB.append`
      BB.word128BE uuid
    )
  <> 
  timestampsBuilder timestamps

encode :: Cauldron -> Builder
encode Cauldron{units,metadata} =
  encodeMetadata metadata
  <>
  Units.encode units

decode :: Bytes -> Maybe Cauldron
decode = parseBytesMaybe parser

takeLebI64Deltas :: Int -> Parser () s (PrimArray Int64)
takeLebI64Deltas !n = do
  dst <- Parser.effect (PM.newPrimArray n)
  let go !lastVal !ix = if ix == n
        then Parser.effect (PM.unsafeFreezePrimArray dst)
        else do
          delta <- Leb128.int64 ()
          let val = lastVal + delta
          Parser.effect (PM.writePrimArray dst ix val)
          go val (ix + 1)
  go 0 0

parser :: Parser () s Cauldron
parser = do
  n <- fmap (fromIntegral @Word32 @Int) (Leb128.word32 ())
  customer <- Leb128.word64 ()
  schema <- Leb128.word64 ()
  origin <- BE.word128 ()
  uuid <- BE.word128 ()
  timestamps <- takeLebI64Deltas n
  let metadata = Metadata{customer,schema,origin,uuid,timestamps}
  units <- Units.parser () n
  pure Cauldron{metadata,units}

-- Precondition: everything is in bounds
multislice ::
     PM.ByteArray -- array
  -> Int -- starting offset
  -> PrimArray Word32 -- sizes
  -> Array Bytes
multislice !b !off0 !szs = runST do
  let !n = PM.sizeofPrimArray szs
  dst <- PM.newArray n mempty
  let go !off !ix = if ix < n
        then do
          let !sz = fromIntegral @Word32 @Int (PM.indexPrimArray szs ix)
              !b' = Bytes b off sz
          PM.writeArray dst ix b'
          go (off + sz) (ix + 1)
        else PM.unsafeFreezeArray dst
  go off0 0
