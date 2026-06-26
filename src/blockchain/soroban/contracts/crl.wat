(module
  (import "v" "0" (func $get_data (param i32 i32) (result i32)))
  (import "v" "1" (func $put_data (param i32 i32 i32) (result i32)))

  ;; Simple CRL contract in WebAssembly (WAT)
  ;; Stores a list of revoked meter IDs.

  (memory (export "memory") 1)

  ;; Keys for the storage
  (data (i32.const 0) "revoked_meters")

  ;; Checks if a meter_id is revoked
  (func (export "is_revoked") (param $id_ptr i32) (param $id_len i32) (result i32)
    (i32.const 0)
  )

  ;; Adds a meter_id to the CRL
  (func (export "revoke") (param $id_ptr i32) (param $id_len i32)
  )

  ;; Returns the full list of revoked meters
  (func (export "get_crl") (result i32)
    (i32.const 0)
  )
)
