CREATE TABLE "SUB" (
  "adsh" varchar(20) PRIMARY KEY,
  "cik" int NOT NULL,
  "name" varchar(150) NOT NULL,
  "sic" int,
  "form" varchar(10) NOT NULL,
  "fy" int,
  "fp" varchar(2),
  "period" date NOT NULL,
  "filed" date NOT NULL,
  "prevrpt" int NOT NULL
);

CREATE TABLE "TAG" (
  "tag" varchar(256) NOT NULL,
  "version" varchar(20) NOT NULL,
  "custom" int NOT NULL,
  "abstract" int NOT NULL,
  "datatype" varchar(20),
  "crdr" varchar(1),
  "iord" varchar(1) NOT NULL,
  PRIMARY KEY ("tag", "version")
);

CREATE TABLE "NUM" (
  "adsh" varchar(20) NOT NULL,
  "tag" varchar(256) NOT NULL,
  "version" varchar(20) NOT NULL,
  "uom" varchar(20) NOT NULL,
  "qtrs" int NOT NULL,
  "ddate" date NOT NULL,
  "segments" varchar(1024),
  "coreg" varchar(256),
  "value" decimal,
  PRIMARY KEY ("adsh", "tag", "version", "ddate", "qtrs", "uom", "segments", "coreg")
);

COMMENT ON COLUMN "SUB"."fy" IS 'Fiscal year (YYYY)';

ALTER TABLE "NUM" ADD FOREIGN KEY ("adsh") REFERENCES "SUB" ("adsh");

ALTER TABLE "NUM" ADD FOREIGN KEY ("tag", "version") REFERENCES "TAG" ("tag", "version");
