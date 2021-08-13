meta:
  id: sstable_compression_info
  endian: be

seq:
  - id: algorithm
    type: string
    doc: LZ4Compressor, SnappyCompressor or DeflateCompressor
  - id: options_count
    type: u4
    doc: Usually no options are passed
  - id: options
    type: option
    repeat: expr
    repeat-expr: options_count
    doc: |
      The only option that exists is "crc_check_chance", which determines the
      probability that we verify the checksum of a compressed chunk we read
  - id: chunk_length
    type: u4
    doc: |
      The size of each chunk that the uncompressed data is split into. Defaults
      to 65536 (1 << 16) but can be any power of 2
  - id: data_length
    type: u8
    doc: The total length of the uncompressed data
  - id: chunk_count
    type: u4
  - id: chunk_offsets
    type: u8
    repeat: expr
    repeat-expr: chunk_count

types:
  string:
    seq:
      - id: len
        type: u2
      - id: value
        size: len
        type: str
        encoding: UTF-8
  option:
    seq:
      - id: key
        type: string
      - id: value
        type: string
