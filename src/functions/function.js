{
  "bindings": [
    {
      "name": "inputBlob",
      "type": "blobTrigger",
      "direction": "in",
      "path": "your-container-name/{name}",
      "connection": "BLOB_CONNECTION_STRING"
    }
  ],
  "scriptFile": "../dist/index.js"
}
