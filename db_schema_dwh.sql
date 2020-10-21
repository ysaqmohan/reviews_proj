--
-- PostgreSQL database dump
--

-- Dumped from database version 10.11
-- Dumped by pg_dump version 10.10 (Ubuntu 10.10-0ubuntu0.18.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: DATABASE "Amazon_DWH"; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON DATABASE "Amazon_DWH" IS 'DWH for Amazon';


--
-- Name: BACKUP_DB; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA "BACKUP_DB";


ALTER SCHEMA "BACKUP_DB" OWNER TO postgres;

--
-- Name: SCHEMA "BACKUP_DB"; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA "BACKUP_DB" IS 'Create DB backups of obselete data';


--
-- Name: CORE_DB; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA "CORE_DB";


ALTER SCHEMA "CORE_DB" OWNER TO postgres;

--
-- Name: SCHEMA "CORE_DB"; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA "CORE_DB" IS 'The Core Dart Mart resides here';


--
-- Name: MAT_DB; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA "MAT_DB";


ALTER SCHEMA "MAT_DB" OWNER TO postgres;

--
-- Name: SCHEMA "MAT_DB"; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA "MAT_DB" IS 'This layer has materialized data for common reports';


--
-- Name: META_DB; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA "META_DB";


ALTER SCHEMA "META_DB" OWNER TO postgres;

--
-- Name: SCHEMA "META_DB"; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA "META_DB" IS 'The schema contains tables for handling load controls';


--
-- Name: STAGE_DB; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA "STAGE_DB";


ALTER SCHEMA "STAGE_DB" OWNER TO postgres;

--
-- Name: SCHEMA "STAGE_DB"; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA "STAGE_DB" IS 'Staging schema';


--
-- Name: WORK_DB; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA "WORK_DB";


ALTER SCHEMA "WORK_DB" OWNER TO postgres;

--
-- Name: SCHEMA "WORK_DB"; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA "WORK_DB" IS 'Work DB space';


--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: prodmeta; Type: TABLE; Schema: BACKUP_DB; Owner: postgres
--

CREATE TABLE "BACKUP_DB".prodmeta (
    asin character varying(15),
    brand character varying(20),
    price numeric(9,2),
    title character varying(100),
    categories character varying(50),
    odsaddtd timestamp without time zone,
    odsupdatetd timestamp without time zone
);


ALTER TABLE "BACKUP_DB".prodmeta OWNER TO postgres;

--
-- Name: reviews_bkp; Type: TABLE; Schema: BACKUP_DB; Owner: postgres
--

CREATE TABLE "BACKUP_DB".reviews_bkp (
    reviewerid character varying(30),
    asin character varying(15),
    reviewername character varying(70),
    reviewrating smallint,
    reviewdate date,
    odsaddtd timestamp without time zone
);


ALTER TABLE "BACKUP_DB".reviews_bkp OWNER TO postgres;

--
-- Name: asin_dim; Type: TABLE; Schema: CORE_DB; Owner: postgres
--

CREATE TABLE "CORE_DB".asin_dim (
    asin_id character varying(15) NOT NULL,
    asin_nm character varying(100),
    brand_nm character varying(20),
    category_id integer,
    add_td timestamp(0) without time zone,
    modified_td timestamp(0) without time zone
);


ALTER TABLE "CORE_DB".asin_dim OWNER TO postgres;

--
-- Name: category_dim; Type: TABLE; Schema: CORE_DB; Owner: postgres
--

CREATE TABLE "CORE_DB".category_dim (
    category_id integer NOT NULL,
    category_nm character varying(50),
    add_td timestamp(0) without time zone,
    modified_td timestamp(0) without time zone
);


ALTER TABLE "CORE_DB".category_dim OWNER TO postgres;

--
-- Name: price_bucket_dim; Type: TABLE; Schema: CORE_DB; Owner: postgres
--

CREATE TABLE "CORE_DB".price_bucket_dim (
    price_bucket_id integer NOT NULL,
    price_bucket_nm character(2) NOT NULL,
    category_id integer NOT NULL,
    bucket_floor_am numeric(9,2),
    bucket_ceiling_am numeric(9,2),
    effective_start_dt date,
    effective_end_dt date,
    active_in character(1),
    add_td timestamp(0) without time zone,
    modified_td timestamp(0) without time zone
);


ALTER TABLE "CORE_DB".price_bucket_dim OWNER TO postgres;

--
-- Name: Category_Price_Bucket; Type: MATERIALIZED VIEW; Schema: CORE_DB; Owner: postgres
--

CREATE MATERIALIZED VIEW "CORE_DB"."Category_Price_Bucket" AS
 SELECT cd.category_nm AS product_category,
    pbd.price_bucket_nm AS price_bucket,
    count(DISTINCT asd.asin_id) AS number_of_products
   FROM (("CORE_DB".category_dim cd
     JOIN "CORE_DB".price_bucket_dim pbd ON ((cd.category_id = pbd.category_id)))
     JOIN "CORE_DB".asin_dim asd ON ((pbd.category_id = asd.category_id)))
  WHERE (pbd.active_in = 'Y'::bpchar)
  GROUP BY cd.category_nm, pbd.price_bucket_nm
  WITH NO DATA;


ALTER TABLE "CORE_DB"."Category_Price_Bucket" OWNER TO postgres;

--
-- Name: MATERIALIZED VIEW "Category_Price_Bucket"; Type: COMMENT; Schema: CORE_DB; Owner: postgres
--

COMMENT ON MATERIALIZED VIEW "CORE_DB"."Category_Price_Bucket" IS 'Materialized view for category and price bucket';


--
-- Name: date_dim; Type: TABLE; Schema: CORE_DB; Owner: postgres
--

CREATE TABLE "CORE_DB".date_dim (
    cal_date date NOT NULL,
    cal_year integer NOT NULL,
    cal_month integer NOT NULL,
    cal_day integer NOT NULL,
    cal_month_nm character varying(9) NOT NULL,
    cal_quarter integer NOT NULL,
    cal_day_of_week integer NOT NULL,
    cal_day_of_quarter integer NOT NULL,
    cal_day_of_year integer NOT NULL,
    cal_week_of_month integer NOT NULL,
    cal_week_of_year integer NOT NULL
);


ALTER TABLE "CORE_DB".date_dim OWNER TO postgres;

--
-- Name: product_reviews; Type: TABLE; Schema: CORE_DB; Owner: postgres
--

CREATE TABLE "CORE_DB".product_reviews (
    reviewer_id character varying(30) NOT NULL,
    asin_id character varying(15) NOT NULL,
    review_dt date NOT NULL,
    category_id integer,
    review_vl smallint,
    price_am numeric(9,2),
    add_td timestamp(0) without time zone,
    modified_td timestamp(0) without time zone,
    unmatched_in character(1)
);


ALTER TABLE "CORE_DB".product_reviews OWNER TO postgres;

--
-- Name: Reviews_Ratings; Type: MATERIALIZED VIEW; Schema: CORE_DB; Owner: postgres
--

CREATE MATERIALIZED VIEW "CORE_DB"."Reviews_Ratings" AS
 SELECT cal.cal_year AS review_year,
    cal.cal_month AS review_month,
    cal.cal_week_of_year AS review_week,
    cal.cal_date AS review_date,
    pr.review_vl AS review_rating,
    count(*) AS number_of_reviews
   FROM ("CORE_DB".product_reviews pr
     JOIN "CORE_DB".date_dim cal ON ((pr.review_dt = cal.cal_date)))
  GROUP BY cal.cal_year, cal.cal_month, cal.cal_week_of_year, cal.cal_date, pr.review_vl
  WITH NO DATA;


ALTER TABLE "CORE_DB"."Reviews_Ratings" OWNER TO postgres;

--
-- Name: MATERIALIZED VIEW "Reviews_Ratings"; Type: COMMENT; Schema: CORE_DB; Owner: postgres
--

COMMENT ON MATERIALIZED VIEW "CORE_DB"."Reviews_Ratings" IS 'Materialized View for generating review ratings against number of reviews.';


--
-- Name: category_dim_category_id_seq; Type: SEQUENCE; Schema: CORE_DB; Owner: postgres
--

CREATE SEQUENCE "CORE_DB".category_dim_category_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE "CORE_DB".category_dim_category_id_seq OWNER TO postgres;

--
-- Name: category_dim_category_id_seq; Type: SEQUENCE OWNED BY; Schema: CORE_DB; Owner: postgres
--

ALTER SEQUENCE "CORE_DB".category_dim_category_id_seq OWNED BY "CORE_DB".category_dim.category_id;


--
-- Name: reviewer_dim; Type: TABLE; Schema: CORE_DB; Owner: postgres
--

CREATE TABLE "CORE_DB".reviewer_dim (
    reviewer_id text,
    reviewer_nm text,
    add_td timestamp without time zone,
    modified_td timestamp without time zone NOT NULL
);


ALTER TABLE "CORE_DB".reviewer_dim OWNER TO postgres;

--
-- Name: clock_price_bucket; Type: TABLE; Schema: META_DB; Owner: postgres
--

CREATE TABLE "META_DB".clock_price_bucket (
    max_price_bucket_id integer
);


ALTER TABLE "META_DB".clock_price_bucket OWNER TO postgres;

--
-- Name: control_tbl; Type: TABLE; Schema: META_DB; Owner: postgres
--

CREATE TABLE "META_DB".control_tbl (
    schema_nm character varying(8) NOT NULL,
    table_nm character varying(20) NOT NULL,
    last_load_td timestamp(0) without time zone
);


ALTER TABLE "META_DB".control_tbl OWNER TO postgres;

--
-- Name: prodmeta; Type: TABLE; Schema: STAGE_DB; Owner: postgres
--

CREATE TABLE "STAGE_DB".prodmeta (
    asin character varying(15),
    brand character varying(20),
    price numeric(9,2) DEFAULT 0,
    title character varying(100),
    categories character varying(50),
    odsaddtd timestamp without time zone DEFAULT CURRENT_TIMESTAMP(0)
);


ALTER TABLE "STAGE_DB".prodmeta OWNER TO postgres;

--
-- Name: reviews; Type: TABLE; Schema: STAGE_DB; Owner: postgres
--

CREATE TABLE "STAGE_DB".reviews (
    reviewerid character varying(30),
    asin character varying(15),
    reviewername character varying(70),
    reviewrating smallint,
    reviewdate date NOT NULL,
    odsaddtd timestamp without time zone DEFAULT CURRENT_TIMESTAMP(0)
);


ALTER TABLE "STAGE_DB".reviews OWNER TO postgres;

--
-- Name: price_bucket_wt; Type: TABLE; Schema: WORK_DB; Owner: postgres
--

CREATE TABLE "WORK_DB".price_bucket_wt (
    price_bucket_nm character(2),
    category_nm character(50),
    bucket_floor_am numeric(9,2),
    bucket_ceiling_am numeric(9,2)
);


ALTER TABLE "WORK_DB".price_bucket_wt OWNER TO postgres;

--
-- Name: prodmeta_wt; Type: TABLE; Schema: WORK_DB; Owner: postgres
--

CREATE TABLE "WORK_DB".prodmeta_wt (
    asin character varying(15) NOT NULL,
    brand character varying(20),
    price numeric(9,2),
    title character varying(100),
    categories character varying(50)
);


ALTER TABLE "WORK_DB".prodmeta_wt OWNER TO postgres;

--
-- Name: reviews_wt; Type: TABLE; Schema: WORK_DB; Owner: postgres
--

CREATE TABLE "WORK_DB".reviews_wt (
    reviewerid character varying(30) NOT NULL,
    asin character varying(15) NOT NULL,
    reviewername character varying(70),
    reviewrating smallint,
    reviewdate date NOT NULL
);


ALTER TABLE "WORK_DB".reviews_wt OWNER TO postgres;

--
-- Name: category_dim category_id; Type: DEFAULT; Schema: CORE_DB; Owner: postgres
--

ALTER TABLE ONLY "CORE_DB".category_dim ALTER COLUMN category_id SET DEFAULT nextval('"CORE_DB".category_dim_category_id_seq'::regclass);


--
-- Name: asin_dim asin_dim_pkey; Type: CONSTRAINT; Schema: CORE_DB; Owner: postgres
--

ALTER TABLE ONLY "CORE_DB".asin_dim
    ADD CONSTRAINT asin_dim_pkey PRIMARY KEY (asin_id);


--
-- Name: category_dim category_dim_pkey; Type: CONSTRAINT; Schema: CORE_DB; Owner: postgres
--

ALTER TABLE ONLY "CORE_DB".category_dim
    ADD CONSTRAINT category_dim_pkey PRIMARY KEY (category_id);


--
-- Name: date_dim date_dim_pkey; Type: CONSTRAINT; Schema: CORE_DB; Owner: postgres
--

ALTER TABLE ONLY "CORE_DB".date_dim
    ADD CONSTRAINT date_dim_pkey PRIMARY KEY (cal_date);


--
-- Name: product_reviews product_reviews_pkey; Type: CONSTRAINT; Schema: CORE_DB; Owner: postgres
--

ALTER TABLE ONLY "CORE_DB".product_reviews
    ADD CONSTRAINT product_reviews_pkey PRIMARY KEY (reviewer_id, asin_id, review_dt);


--
-- Name: control_tbl control_tbl_pkey; Type: CONSTRAINT; Schema: META_DB; Owner: postgres
--

ALTER TABLE ONLY "META_DB".control_tbl
    ADD CONSTRAINT control_tbl_pkey PRIMARY KEY (schema_nm, table_nm);


--
-- Name: prodmeta_wt prodmeta_wt_pkey; Type: CONSTRAINT; Schema: WORK_DB; Owner: postgres
--

ALTER TABLE ONLY "WORK_DB".prodmeta_wt
    ADD CONSTRAINT prodmeta_wt_pkey PRIMARY KEY (asin);


--
-- Name: reviews_wt reviews_staging_pkey; Type: CONSTRAINT; Schema: WORK_DB; Owner: postgres
--

ALTER TABLE ONLY "WORK_DB".reviews_wt
    ADD CONSTRAINT reviews_staging_pkey PRIMARY KEY (reviewerid, asin, reviewdate);


--
-- Name: prodmeta_odsaddtd_idx; Type: INDEX; Schema: STAGE_DB; Owner: postgres
--

CREATE INDEX prodmeta_odsaddtd_idx ON "STAGE_DB".prodmeta USING btree (odsaddtd);


--
-- Name: reviews_odsaddtd_idx; Type: INDEX; Schema: STAGE_DB; Owner: postgres
--

CREATE INDEX reviews_odsaddtd_idx ON "STAGE_DB".reviews USING btree (odsaddtd);


--
-- PostgreSQL database dump complete
--

