{-# language BangPatterns #-}
{-# language BlockArguments #-}
{-# language LambdaCase #-}
{-# language NamedFieldPuns #-}
{-# language TypeApplications #-}

module Cauldron.Units
  ( encode
  , parser
    -- * Read-only fields
  , presentations_
  , messages_
  , compressed_
  , compression_
  , decompressedSize_
  , sizes_
  , offsets_
  ) where

import Control.Monad.ST (runST)
import Control.Monad (when)
import Data.Primitive (PrimArray,Array)
import Data.Word (Word,Word8,Word32)
import Data.Int (Int64)
import Data.Bytes.Builder (Builder)
import Data.Bytes.Types (Bytes(Bytes))
import Data.Bytes.Parser (Parser)
import Cauldron.Unsafe (Units(..),Compression(..),Messages(..))

import qualified Arithmetic.Nat as Nat
import qualified Data.Bytes as Bytes
import qualified Data.Bytes.Builder as Builder
import qualified Data.Bytes.Parser.Leb128 as Leb128
import qualified Data.Bytes.Builder.Bounded as BB
import qualified Data.Bytes.Parser as Parser
import qualified Data.Primitive as PM
import qualified Json
import qualified Data.Primitive.Contiguous as C
import qualified Lz4.Block as Lz4

encode :: Units -> Builder
encode Units{compression,header,offsets,sizes,decompressedSize,compressed,format} =
  Builder.fromBounded Nat.constant
    ( BB.word8 (encodeCompression compression) `BB.append`
      BB.word8 format `BB.append`
      BB.wordLEB128 (fromIntegral @Int @Word header) `BB.append`
      BB.wordLEB128 (fromIntegral @Int @Word (Bytes.length compressed)) `BB.append`
      BB.wordLEB128 (fromIntegral @Int @Word decompressedSize)
    )
  <>
  sizesBuilder offsets
  <>
  sizesBuilder sizes
  <>
  Builder.bytes compressed

encodeCompression :: Compression -> Word8
encodeCompression = \case
  Lz4 -> 1
  _ -> 0

sizesBuilder :: PrimArray Word32 -> Builder
sizesBuilder = foldlrPrimArray
  (\_ w -> fromIntegral @Word32 @Int w)
  ( \ !prevLen !curLen bldr ->
    let !deltaLen = fromIntegral @Word32 @Int curLen - prevLen
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

-- | Parse the requested number of units.
parser :: e -> Int -> Parser e s Units
parser e !n = do
  compression <- Parser.any e >>= \case
    0 -> pure None
    1 -> pure Lz4
    _ -> Parser.fail e
  format <- Parser.any e
  header <- fmap (fromIntegral @Word32 @Int) (Leb128.word32 e)
  compressedSz <- fmap (fromIntegral @Word32 @Int) (Leb128.word32 e)
  decompressedSize <- fmap (fromIntegral @Word32 @Int) (Leb128.word32 e)
  offsets <- takeLebW32Deltas e n
  sizes <- takeLebW32Deltas e n
  when (hasOutOfBounds decompressedSize offsets sizes) (Parser.fail e)
  compressed <- Parser.take e compressedSz
  when (header /= 0) (Parser.fail e)
  case format of
    1 -> case compression of
      None -> if decompressedSize == compressedSz
        then do
          let slices = case compressed of
                Bytes arr off _ -> multisliceOffsets arr off offsets sizes
              presentations = slices
              messages = Plaintext slices
              units = Units{compression,format,compressed,sizes,header,messages,presentations,decompressedSize,offsets}
          pure units
        else Parser.fail e
      Lz4 -> 
        let ~(presentations,messages) = case Lz4.decompressU decompressedSize compressed of
              Nothing -> (C.replicate n Bytes.empty, Garbage n)
              Just decompressed -> let slices = multisliceOffsets decompressed 0 offsets sizes in
                (slices,Plaintext slices)
            units = Units{compression,format,compressed,sizes,header,messages,presentations,decompressedSize,offsets}
         in pure units
    2 -> case compression of
      None -> if decompressedSize == compressedSz
        then do
          let slices = case compressed of
                Bytes arr off _ -> multisliceOffsets arr off offsets sizes
              presentations = slices
              messages = case traverse Json.decode slices of
                Left _ -> Garbage n
                Right r -> Json r
              units = Units{compression,format,compressed,sizes,header,messages,presentations,decompressedSize,offsets}
          pure units
        else Parser.fail e
      Lz4 -> 
        let ~(presentations,messages) = case Lz4.decompressU decompressedSize compressed of
              Nothing -> (C.replicate n Bytes.empty, Garbage n)
              Just decompressed ->
                let slices = multisliceOffsets decompressed 0 offsets sizes
                 in ( slices
                    , case traverse Json.decode slices of
                        Left _ -> Garbage n
                        Right r -> Json r
                    )
            units = Units{compression,format,compressed,sizes,header,messages,presentations,decompressedSize,offsets}
         in pure units
    _ -> Parser.fail e

hasOutOfBounds :: Int -> PrimArray Word32 -> PrimArray Word32 -> Bool
hasOutOfBounds !totalLen !offs !lens = 
  let go !ix = if ix /= (-1)
        then
          let !off = fromIntegral @Word32 @Int (PM.indexPrimArray offs ix)
              !len = fromIntegral @Word32 @Int (PM.indexPrimArray lens ix)
           in (off + len <= totalLen) && go (ix + 1)
        else False
   in go (PM.sizeofPrimArray offs - 1)

-- Precondition: everything is in bounds
multisliceOffsets ::
     PM.ByteArray -- array
  -> Int -- extra offset apply to everything
  -> PrimArray Word32 -- offsets
  -> PrimArray Word32 -- lengths, must be same length as offsets
  -> Array Bytes
multisliceOffsets !b !baseOff !offs !lens = runST do
  let !n = PM.sizeofPrimArray lens
  dst <- PM.newArray n mempty
  let go !ix = if ix < n
        then do
          let !off = fromIntegral @Word32 @Int (PM.indexPrimArray offs ix)
              !len = fromIntegral @Word32 @Int (PM.indexPrimArray lens ix)
              !b' = Bytes b (off + baseOff) len
          PM.writeArray dst ix b'
          go (ix + 1)
        else PM.unsafeFreezeArray dst
  go 0

takeLebW32Deltas :: e -> Int -> Parser e s (PrimArray Word32)
takeLebW32Deltas e !n = do
  dst <- Parser.effect (PM.newPrimArray n)
  let go !lastVal !ix = if ix == n
        then Parser.effect (PM.unsafeFreezePrimArray dst)
        else do
          delta <- Leb128.int64 e
          let val = lastVal + delta
          if val < 0 || val > 0xFFFFFFFF
            then Parser.fail e
            else do
              Parser.effect (PM.writePrimArray dst ix (fromIntegral @Int64 @Word32 val))
              go val (ix + 1)
  go 0 0

presentations_ :: Units -> Array Bytes
presentations_ Units{presentations} = presentations

messages_ :: Units -> Messages
messages_ Units{messages} = messages

compression_ :: Units -> Compression
compression_ Units{compression} = compression

decompressedSize_ :: Units -> Int
decompressedSize_ Units{decompressedSize} = decompressedSize

compressed_ :: Units -> Bytes
compressed_ Units{compressed} = compressed

sizes_ :: Units -> PrimArray Word32
sizes_ Units{sizes} = sizes

offsets_ :: Units -> PrimArray Word32
offsets_ Units{offsets} = offsets
