module Cauldron.Unsafe
  ( Cauldron(..)
  , Compression(..)
  , Messages(..)
  , Metadata(..)
  , Units(..)
  ) where

import Data.Int (Int64)
import Data.WideWord (Word128)
import Data.Word (Word32,Word64,Word8)
import Data.Primitive (PrimArray,Array)
import Data.Bytes (Bytes)

import qualified Json

data Compression
  = None
  | Lz4

data Messages
  = Plaintext !(Array Bytes)
  | Json !(Array Json.Value) -- Decoded JSON values
  | Garbage !Int
  deriving (Eq)

data Cauldron = Cauldron
  { metadata :: !Metadata
    -- ^ Timestamps (nanoseconds since epoch)
  , units :: !Units
    -- ^ Raw logs
  }

data Metadata = Metadata
  { customer :: !Word64
    -- ^ Numeric identifier of customer
  , schema :: !Word64
    -- ^ Numeric schema identifier, helps parse data
  , uuid :: {-# UNPACK #-} !Word128
    -- ^ UUID of batch. Common interpretation is that records in
    -- batch have ascending UUIDs starting at this one. 
  , timestamps :: !(PrimArray Int64)
    -- ^ Timestamps (nanoseconds since epoch)
  } deriving (Eq)

data Units = Units
  { compression :: !Compression
    -- ^ Compression scheme
  , format :: !Word8
    -- ^ Encoded format. Must agree with messages.
  , messages :: Messages
    -- ^ Decompressed parsed messages. Lazy.
  , presentations :: Array Bytes
    -- ^ Human-readable encoded messages. For JSON, this is just
    -- the normal presentation of JSON. Lazy.
  , compressed :: {-# UNPACK #-} !Bytes
    -- ^ Compressed messages.
  , header :: !Int
    -- ^ Length of header. Often zero.
  , sizes :: !(PrimArray Word32)
    -- ^ Lengths of decompressed messages. Must be in bounds.
    -- Represented as 32-bit words to save a little space.
  }
