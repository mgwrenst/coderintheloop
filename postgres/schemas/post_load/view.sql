DROP VIEW IF EXISTS alleSelskaper;

CREATE VIEW alleSelskaper AS
SELECT *
FROM selskap
UNION ALL
SELECT *
FROM konkurs;