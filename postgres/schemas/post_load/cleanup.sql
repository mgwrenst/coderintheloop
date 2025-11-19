DELETE FROM selskap
WHERE uuid IN (SELECT uuid FROM konkurs);