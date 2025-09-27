```mermaid
---  
title: "Log Record Format"
---
packet-beta
0-31: "checksum (uint32, crc32c)"
32-47: "length (uint16)"
48-55: "type (uint8)"
56-63: "data[0]"
64-71: "data[1]"
72-79: "data[2]"
80-87: "..."
88-95: "data[length-1]"

```

```plaintext
block := record* trailer?
record :=
  checksum: uint32     // crc32c of type and data[] ; little-endian
  length: uint16       // little-endian
  type: uint8          // One of FULL, FIRST, MIDDLE, LAST
  data: uint8[length]
```
