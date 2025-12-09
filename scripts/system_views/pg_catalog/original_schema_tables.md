-- pg_catalog.pg_aggregate definition

-- Drop table

-- DROP TABLE pg_catalog.pg_aggregate;

CREATE TABLE pg_catalog.pg_aggregate (
	aggfnoid regproc NOT NULL,
	aggkind char NOT NULL,
	aggnumdirectargs int2 NOT NULL,
	aggtransfn regproc NOT NULL,
	aggfinalfn regproc NOT NULL,
	aggcombinefn regproc NOT NULL,
	aggserialfn regproc NOT NULL,
	aggdeserialfn regproc NOT NULL,
	aggmtransfn regproc NOT NULL,
	aggminvtransfn regproc NOT NULL,
	aggmfinalfn regproc NOT NULL,
	aggfinalextra bool NOT NULL,
	aggmfinalextra bool NOT NULL,
	aggfinalmodify char NOT NULL,
	aggmfinalmodify char NOT NULL,
	aggsortop oid NOT NULL,
	aggtranstype oid NOT NULL,
	aggtransspace int4 NOT NULL,
	aggmtranstype oid NOT NULL,
	aggmtransspace int4 NOT NULL,
	agginitval text COLLATE "C" NULL,
	aggminitval text COLLATE "C" NULL,
	CONSTRAINT pg_aggregate_fnoid_index PRIMARY KEY (aggfnoid)
);


-- pg_catalog.pg_am definition

-- Drop table

-- DROP TABLE pg_catalog.pg_am;

CREATE TABLE pg_catalog.pg_am (
	"oid" oid NOT NULL,
	amname name COLLATE "C" NOT NULL,
	amhandler regproc NOT NULL,
	amtype char NOT NULL,
	CONSTRAINT pg_am_name_index UNIQUE (amname),
	CONSTRAINT pg_am_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_amop definition

-- Drop table

-- DROP TABLE pg_catalog.pg_amop;

CREATE TABLE pg_catalog.pg_amop (
	"oid" oid NOT NULL,
	amopfamily oid NOT NULL,
	amoplefttype oid NOT NULL,
	amoprighttype oid NOT NULL,
	amopstrategy int2 NOT NULL,
	amoppurpose char NOT NULL,
	amopopr oid NOT NULL,
	amopmethod oid NOT NULL,
	amopsortfamily oid NOT NULL,
	CONSTRAINT pg_amop_fam_strat_index UNIQUE (amopfamily, amoplefttype, amoprighttype, amopstrategy),
	CONSTRAINT pg_amop_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_amop_opr_fam_index UNIQUE (amopopr, amoppurpose, amopfamily)
);


-- pg_catalog.pg_amproc definition

-- Drop table

-- DROP TABLE pg_catalog.pg_amproc;

CREATE TABLE pg_catalog.pg_amproc (
	"oid" oid NOT NULL,
	amprocfamily oid NOT NULL,
	amproclefttype oid NOT NULL,
	amprocrighttype oid NOT NULL,
	amprocnum int2 NOT NULL,
	amproc regproc NOT NULL,
	CONSTRAINT pg_amproc_fam_proc_index UNIQUE (amprocfamily, amproclefttype, amprocrighttype, amprocnum),
	CONSTRAINT pg_amproc_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_attrdef definition

-- Drop table

-- DROP TABLE pg_catalog.pg_attrdef;

CREATE TABLE pg_catalog.pg_attrdef (
	"oid" oid NOT NULL,
	adrelid oid NOT NULL,
	adnum int2 NOT NULL,
	adbin pg_node_tree COLLATE "C" NOT NULL,
	CONSTRAINT pg_attrdef_adrelid_adnum_index UNIQUE (adrelid, adnum),
	CONSTRAINT pg_attrdef_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_attribute definition

-- Drop table

-- DROP TABLE pg_catalog.pg_attribute;

CREATE TABLE pg_catalog.pg_attribute (
	attrelid oid NOT NULL,
	attname name COLLATE "C" NOT NULL,
	atttypid oid NOT NULL,
	attstattarget int4 NOT NULL,
	attlen int2 NOT NULL,
	attnum int2 NOT NULL,
	attndims int4 NOT NULL,
	attcacheoff int4 NOT NULL,
	atttypmod int4 NOT NULL,
	attbyval bool NOT NULL,
	attalign char NOT NULL,
	attstorage char NOT NULL,
	attcompression char NOT NULL,
	attnotnull bool NOT NULL,
	atthasdef bool NOT NULL,
	atthasmissing bool NOT NULL,
	attidentity char NOT NULL,
	attgenerated char NOT NULL,
	attisdropped bool NOT NULL,
	attislocal bool NOT NULL,
	attinhcount int4 NOT NULL,
	attcollation oid NOT NULL,
	attacl _aclitem NULL,
	attoptions _text COLLATE "C" NULL,
	attfdwoptions _text COLLATE "C" NULL,
	attmissingval anyarray NULL,
	CONSTRAINT pg_attribute_relid_attnam_index UNIQUE (attrelid, attname),
	CONSTRAINT pg_attribute_relid_attnum_index PRIMARY KEY (attrelid, attnum)
);


-- pg_catalog.pg_auth_members definition

-- Drop table

-- DROP TABLE pg_catalog.pg_auth_members;

CREATE TABLE pg_catalog.pg_auth_members (
	roleid oid NOT NULL,
	"member" oid NOT NULL,
	grantor oid NOT NULL,
	admin_option bool NOT NULL,
	CONSTRAINT pg_auth_members_member_role_index UNIQUE (member, roleid),
	CONSTRAINT pg_auth_members_role_member_index PRIMARY KEY (roleid, member)
)
TABLESPACE pg_global
;


-- pg_catalog.pg_authid definition

-- Drop table

-- DROP TABLE pg_catalog.pg_authid;

CREATE TABLE pg_catalog.pg_authid (
	"oid" oid NOT NULL,
	rolname name COLLATE "C" NOT NULL,
	rolsuper bool NOT NULL,
	rolinherit bool NOT NULL,
	rolcreaterole bool NOT NULL,
	rolcreatedb bool NOT NULL,
	rolcanlogin bool NOT NULL,
	rolreplication bool NOT NULL,
	rolbypassrls bool NOT NULL,
	rolconnlimit int4 NOT NULL,
	rolpassword text COLLATE "C" NULL,
	rolvaliduntil timestamptz NULL,
	CONSTRAINT pg_authid_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_authid_rolname_index UNIQUE (rolname)
)
TABLESPACE pg_global
;


-- pg_catalog.pg_cast definition

-- Drop table

-- DROP TABLE pg_catalog.pg_cast;

CREATE TABLE pg_catalog.pg_cast (
	"oid" oid NOT NULL,
	castsource oid NOT NULL,
	casttarget oid NOT NULL,
	castfunc oid NOT NULL,
	castcontext char NOT NULL,
	castmethod char NOT NULL,
	CONSTRAINT pg_cast_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_cast_source_target_index UNIQUE (castsource, casttarget)
);


-- pg_catalog.pg_class definition

-- Drop table

-- DROP TABLE pg_catalog.pg_class;

CREATE TABLE pg_catalog.pg_class (
	"oid" oid NOT NULL,
	relname name COLLATE "C" NOT NULL,
	relnamespace oid NOT NULL,
	reltype oid NOT NULL,
	reloftype oid NOT NULL,
	relowner oid NOT NULL,
	relam oid NOT NULL,
	relfilenode oid NOT NULL,
	reltablespace oid NOT NULL,
	relpages int4 NOT NULL,
	reltuples float4 NOT NULL,
	relallvisible int4 NOT NULL,
	reltoastrelid oid NOT NULL,
	relhasindex bool NOT NULL,
	relisshared bool NOT NULL,
	relpersistence char NOT NULL,
	relkind char NOT NULL,
	relnatts int2 NOT NULL,
	relchecks int2 NOT NULL,
	relhasrules bool NOT NULL,
	relhastriggers bool NOT NULL,
	relhassubclass bool NOT NULL,
	relrowsecurity bool NOT NULL,
	relforcerowsecurity bool NOT NULL,
	relispopulated bool NOT NULL,
	relreplident char NOT NULL,
	relispartition bool NOT NULL,
	relrewrite oid NOT NULL,
	relfrozenxid xid NOT NULL,
	relminmxid xid NOT NULL,
	relacl _aclitem NULL,
	reloptions _text COLLATE "C" NULL,
	relpartbound pg_node_tree COLLATE "C" NULL,
	CONSTRAINT pg_class_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_class_relname_nsp_index UNIQUE (relname, relnamespace)
);
CREATE INDEX pg_class_tblspc_relfilenode_index ON pg_catalog.pg_class USING btree (reltablespace, relfilenode);


-- pg_catalog.pg_collation definition

-- Drop table

-- DROP TABLE pg_catalog.pg_collation;

CREATE TABLE pg_catalog.pg_collation (
	"oid" oid NOT NULL,
	collname name COLLATE "C" NOT NULL,
	collnamespace oid NOT NULL,
	collowner oid NOT NULL,
	collprovider char NOT NULL,
	collisdeterministic bool NOT NULL,
	collencoding int4 NOT NULL,
	collcollate name COLLATE "C" NOT NULL,
	collctype name COLLATE "C" NOT NULL,
	collversion text COLLATE "C" NULL,
	CONSTRAINT pg_collation_name_enc_nsp_index UNIQUE (collname, collencoding, collnamespace),
	CONSTRAINT pg_collation_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_constraint definition

-- Drop table

-- DROP TABLE pg_catalog.pg_constraint;

CREATE TABLE pg_catalog.pg_constraint (
	"oid" oid NOT NULL,
	conname name COLLATE "C" NOT NULL,
	connamespace oid NOT NULL,
	contype char NOT NULL,
	condeferrable bool NOT NULL,
	condeferred bool NOT NULL,
	convalidated bool NOT NULL,
	conrelid oid NOT NULL,
	contypid oid NOT NULL,
	conindid oid NOT NULL,
	conparentid oid NOT NULL,
	confrelid oid NOT NULL,
	confupdtype char NOT NULL,
	confdeltype char NOT NULL,
	confmatchtype char NOT NULL,
	conislocal bool NOT NULL,
	coninhcount int4 NOT NULL,
	connoinherit bool NOT NULL,
	conkey _int2 NULL,
	confkey _int2 NULL,
	conpfeqop _oid NULL,
	conppeqop _oid NULL,
	conffeqop _oid NULL,
	conexclop _oid NULL,
	conbin pg_node_tree COLLATE "C" NULL,
	CONSTRAINT pg_constraint_conrelid_contypid_conname_index UNIQUE (conrelid, contypid, conname),
	CONSTRAINT pg_constraint_oid_index PRIMARY KEY (oid)
);
CREATE INDEX pg_constraint_conname_nsp_index ON pg_catalog.pg_constraint USING btree (conname, connamespace);
CREATE INDEX pg_constraint_conparentid_index ON pg_catalog.pg_constraint USING btree (conparentid);
CREATE INDEX pg_constraint_contypid_index ON pg_catalog.pg_constraint USING btree (contypid);


-- pg_catalog.pg_conversion definition

-- Drop table

-- DROP TABLE pg_catalog.pg_conversion;

CREATE TABLE pg_catalog.pg_conversion (
	"oid" oid NOT NULL,
	conname name COLLATE "C" NOT NULL,
	connamespace oid NOT NULL,
	conowner oid NOT NULL,
	conforencoding int4 NOT NULL,
	contoencoding int4 NOT NULL,
	conproc regproc NOT NULL,
	condefault bool NOT NULL,
	CONSTRAINT pg_conversion_default_index UNIQUE (connamespace, conforencoding, contoencoding, oid),
	CONSTRAINT pg_conversion_name_nsp_index UNIQUE (conname, connamespace),
	CONSTRAINT pg_conversion_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_database definition

-- Drop table

-- DROP TABLE pg_catalog.pg_database;

CREATE TABLE pg_catalog.pg_database (
	"oid" oid NOT NULL,
	datname name COLLATE "C" NOT NULL,
	datdba oid NOT NULL,
	"encoding" int4 NOT NULL,
	datcollate name COLLATE "C" NOT NULL,
	datctype name COLLATE "C" NOT NULL,
	datistemplate bool NOT NULL,
	datallowconn bool NOT NULL,
	datconnlimit int4 NOT NULL,
	datlastsysoid oid NOT NULL,
	datfrozenxid xid NOT NULL,
	datminmxid xid NOT NULL,
	dattablespace oid NOT NULL,
	datacl _aclitem NULL,
	CONSTRAINT pg_database_datname_index UNIQUE (datname),
	CONSTRAINT pg_database_oid_index PRIMARY KEY (oid)
)
TABLESPACE pg_global
;


-- pg_catalog.pg_db_role_setting definition

-- Drop table

-- DROP TABLE pg_catalog.pg_db_role_setting;

CREATE TABLE pg_catalog.pg_db_role_setting (
	setdatabase oid NOT NULL,
	setrole oid NOT NULL,
	setconfig _text COLLATE "C" NULL,
	CONSTRAINT pg_db_role_setting_databaseid_rol_index PRIMARY KEY (setdatabase, setrole)
)
TABLESPACE pg_global
;


-- pg_catalog.pg_default_acl definition

-- Drop table

-- DROP TABLE pg_catalog.pg_default_acl;

CREATE TABLE pg_catalog.pg_default_acl (
	"oid" oid NOT NULL,
	defaclrole oid NOT NULL,
	defaclnamespace oid NOT NULL,
	defaclobjtype char NOT NULL,
	defaclacl _aclitem NOT NULL,
	CONSTRAINT pg_default_acl_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_default_acl_role_nsp_obj_index UNIQUE (defaclrole, defaclnamespace, defaclobjtype)
);


-- pg_catalog.pg_depend definition

-- Drop table

-- DROP TABLE pg_catalog.pg_depend;

CREATE TABLE pg_catalog.pg_depend (
	classid oid NOT NULL,
	objid oid NOT NULL,
	objsubid int4 NOT NULL,
	refclassid oid NOT NULL,
	refobjid oid NOT NULL,
	refobjsubid int4 NOT NULL,
	deptype char NOT NULL
);
CREATE INDEX pg_depend_depender_index ON pg_catalog.pg_depend USING btree (classid, objid, objsubid);
CREATE INDEX pg_depend_reference_index ON pg_catalog.pg_depend USING btree (refclassid, refobjid, refobjsubid);


-- pg_catalog.pg_description definition

-- Drop table

-- DROP TABLE pg_catalog.pg_description;

CREATE TABLE pg_catalog.pg_description (
	objoid oid NOT NULL,
	classoid oid NOT NULL,
	objsubid int4 NOT NULL,
	description text COLLATE "C" NOT NULL,
	CONSTRAINT pg_description_o_c_o_index PRIMARY KEY (objoid, classoid, objsubid)
);


-- pg_catalog.pg_enum definition

-- Drop table

-- DROP TABLE pg_catalog.pg_enum;

CREATE TABLE pg_catalog.pg_enum (
	"oid" oid NOT NULL,
	enumtypid oid NOT NULL,
	enumsortorder float4 NOT NULL,
	enumlabel name COLLATE "C" NOT NULL,
	CONSTRAINT pg_enum_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_enum_typid_label_index UNIQUE (enumtypid, enumlabel),
	CONSTRAINT pg_enum_typid_sortorder_index UNIQUE (enumtypid, enumsortorder)
);


-- pg_catalog.pg_event_trigger definition

-- Drop table

-- DROP TABLE pg_catalog.pg_event_trigger;

CREATE TABLE pg_catalog.pg_event_trigger (
	"oid" oid NOT NULL,
	evtname name COLLATE "C" NOT NULL,
	evtevent name COLLATE "C" NOT NULL,
	evtowner oid NOT NULL,
	evtfoid oid NOT NULL,
	evtenabled char NOT NULL,
	evttags _text COLLATE "C" NULL,
	CONSTRAINT pg_event_trigger_evtname_index UNIQUE (evtname),
	CONSTRAINT pg_event_trigger_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_extension definition

-- Drop table

-- DROP TABLE pg_catalog.pg_extension;

CREATE TABLE pg_catalog.pg_extension (
	"oid" oid NOT NULL,
	extname name COLLATE "C" NOT NULL,
	extowner oid NOT NULL,
	extnamespace oid NOT NULL,
	extrelocatable bool NOT NULL,
	extversion text COLLATE "C" NOT NULL,
	extconfig _oid NULL,
	extcondition _text COLLATE "C" NULL,
	CONSTRAINT pg_extension_name_index UNIQUE (extname),
	CONSTRAINT pg_extension_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_foreign_data_wrapper definition

-- Drop table

-- DROP TABLE pg_catalog.pg_foreign_data_wrapper;

CREATE TABLE pg_catalog.pg_foreign_data_wrapper (
	"oid" oid NOT NULL,
	fdwname name COLLATE "C" NOT NULL,
	fdwowner oid NOT NULL,
	fdwhandler oid NOT NULL,
	fdwvalidator oid NOT NULL,
	fdwacl _aclitem NULL,
	fdwoptions _text COLLATE "C" NULL,
	CONSTRAINT pg_foreign_data_wrapper_name_index UNIQUE (fdwname),
	CONSTRAINT pg_foreign_data_wrapper_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_foreign_server definition

-- Drop table

-- DROP TABLE pg_catalog.pg_foreign_server;

CREATE TABLE pg_catalog.pg_foreign_server (
	"oid" oid NOT NULL,
	srvname name COLLATE "C" NOT NULL,
	srvowner oid NOT NULL,
	srvfdw oid NOT NULL,
	srvtype text COLLATE "C" NULL,
	srvversion text COLLATE "C" NULL,
	srvacl _aclitem NULL,
	srvoptions _text COLLATE "C" NULL,
	CONSTRAINT pg_foreign_server_name_index UNIQUE (srvname),
	CONSTRAINT pg_foreign_server_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_foreign_table definition

-- Drop table

-- DROP TABLE pg_catalog.pg_foreign_table;

CREATE TABLE pg_catalog.pg_foreign_table (
	ftrelid oid NOT NULL,
	ftserver oid NOT NULL,
	ftoptions _text COLLATE "C" NULL,
	CONSTRAINT pg_foreign_table_relid_index PRIMARY KEY (ftrelid)
);


-- pg_catalog.pg_index definition

-- Drop table

-- DROP TABLE pg_catalog.pg_index;

CREATE TABLE pg_catalog.pg_index (
	indexrelid oid NOT NULL,
	indrelid oid NOT NULL,
	indnatts int2 NOT NULL,
	indnkeyatts int2 NOT NULL,
	indisunique bool NOT NULL,
	indisprimary bool NOT NULL,
	indisexclusion bool NOT NULL,
	indimmediate bool NOT NULL,
	indisclustered bool NOT NULL,
	indisvalid bool NOT NULL,
	indcheckxmin bool NOT NULL,
	indisready bool NOT NULL,
	indislive bool NOT NULL,
	indisreplident bool NOT NULL,
	indkey int2vector NOT NULL,
	indcollation oidvector NOT NULL,
	indclass oidvector NOT NULL,
	indoption int2vector NOT NULL,
	indexprs pg_node_tree COLLATE "C" NULL,
	indpred pg_node_tree COLLATE "C" NULL,
	CONSTRAINT pg_index_indexrelid_index PRIMARY KEY (indexrelid)
);
CREATE INDEX pg_index_indrelid_index ON pg_catalog.pg_index USING btree (indrelid);


-- pg_catalog.pg_inherits definition

-- Drop table

-- DROP TABLE pg_catalog.pg_inherits;

CREATE TABLE pg_catalog.pg_inherits (
	inhrelid oid NOT NULL,
	inhparent oid NOT NULL,
	inhseqno int4 NOT NULL,
	inhdetachpending bool NOT NULL,
	CONSTRAINT pg_inherits_relid_seqno_index PRIMARY KEY (inhrelid, inhseqno)
);
CREATE INDEX pg_inherits_parent_index ON pg_catalog.pg_inherits USING btree (inhparent);


-- pg_catalog.pg_init_privs definition

-- Drop table

-- DROP TABLE pg_catalog.pg_init_privs;

CREATE TABLE pg_catalog.pg_init_privs (
	objoid oid NOT NULL,
	classoid oid NOT NULL,
	objsubid int4 NOT NULL,
	privtype char NOT NULL,
	initprivs _aclitem NOT NULL,
	CONSTRAINT pg_init_privs_o_c_o_index PRIMARY KEY (objoid, classoid, objsubid)
);


-- pg_catalog.pg_language definition

-- Drop table

-- DROP TABLE pg_catalog.pg_language;

CREATE TABLE pg_catalog.pg_language (
	"oid" oid NOT NULL,
	lanname name COLLATE "C" NOT NULL,
	lanowner oid NOT NULL,
	lanispl bool NOT NULL,
	lanpltrusted bool NOT NULL,
	lanplcallfoid oid NOT NULL,
	laninline oid NOT NULL,
	lanvalidator oid NOT NULL,
	lanacl _aclitem NULL,
	CONSTRAINT pg_language_name_index UNIQUE (lanname),
	CONSTRAINT pg_language_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_largeobject definition

-- Drop table

-- DROP TABLE pg_catalog.pg_largeobject;

CREATE TABLE pg_catalog.pg_largeobject (
	loid oid NOT NULL,
	pageno int4 NOT NULL,
	"data" bytea NOT NULL,
	CONSTRAINT pg_largeobject_loid_pn_index PRIMARY KEY (loid, pageno)
);


-- pg_catalog.pg_largeobject_metadata definition

-- Drop table

-- DROP TABLE pg_catalog.pg_largeobject_metadata;

CREATE TABLE pg_catalog.pg_largeobject_metadata (
	"oid" oid NOT NULL,
	lomowner oid NOT NULL,
	lomacl _aclitem NULL,
	CONSTRAINT pg_largeobject_metadata_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_namespace definition

-- Drop table

-- DROP TABLE pg_catalog.pg_namespace;

CREATE TABLE pg_catalog.pg_namespace (
	"oid" oid NOT NULL,
	nspname name COLLATE "C" NOT NULL,
	nspowner oid NOT NULL,
	nspacl _aclitem NULL,
	CONSTRAINT pg_namespace_nspname_index UNIQUE (nspname),
	CONSTRAINT pg_namespace_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_opclass definition

-- Drop table

-- DROP TABLE pg_catalog.pg_opclass;

CREATE TABLE pg_catalog.pg_opclass (
	"oid" oid NOT NULL,
	opcmethod oid NOT NULL,
	opcname name COLLATE "C" NOT NULL,
	opcnamespace oid NOT NULL,
	opcowner oid NOT NULL,
	opcfamily oid NOT NULL,
	opcintype oid NOT NULL,
	opcdefault bool NOT NULL,
	opckeytype oid NOT NULL,
	CONSTRAINT pg_opclass_am_name_nsp_index UNIQUE (opcmethod, opcname, opcnamespace),
	CONSTRAINT pg_opclass_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_operator definition

-- Drop table

-- DROP TABLE pg_catalog.pg_operator;

CREATE TABLE pg_catalog.pg_operator (
	"oid" oid NOT NULL,
	oprname name COLLATE "C" NOT NULL,
	oprnamespace oid NOT NULL,
	oprowner oid NOT NULL,
	oprkind char NOT NULL,
	oprcanmerge bool NOT NULL,
	oprcanhash bool NOT NULL,
	oprleft oid NOT NULL,
	oprright oid NOT NULL,
	oprresult oid NOT NULL,
	oprcom oid NOT NULL,
	oprnegate oid NOT NULL,
	oprcode regproc NOT NULL,
	oprrest regproc NOT NULL,
	oprjoin regproc NOT NULL,
	CONSTRAINT pg_operator_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_operator_oprname_l_r_n_index UNIQUE (oprname, oprleft, oprright, oprnamespace)
);


-- pg_catalog.pg_opfamily definition

-- Drop table

-- DROP TABLE pg_catalog.pg_opfamily;

CREATE TABLE pg_catalog.pg_opfamily (
	"oid" oid NOT NULL,
	opfmethod oid NOT NULL,
	opfname name COLLATE "C" NOT NULL,
	opfnamespace oid NOT NULL,
	opfowner oid NOT NULL,
	CONSTRAINT pg_opfamily_am_name_nsp_index UNIQUE (opfmethod, opfname, opfnamespace),
	CONSTRAINT pg_opfamily_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_partitioned_table definition

-- Drop table

-- DROP TABLE pg_catalog.pg_partitioned_table;

CREATE TABLE pg_catalog.pg_partitioned_table (
	partrelid oid NOT NULL,
	partstrat char NOT NULL,
	partnatts int2 NOT NULL,
	partdefid oid NOT NULL,
	partattrs int2vector NOT NULL,
	partclass oidvector NOT NULL,
	partcollation oidvector NOT NULL,
	partexprs pg_node_tree COLLATE "C" NULL,
	CONSTRAINT pg_partitioned_table_partrelid_index PRIMARY KEY (partrelid)
);


-- pg_catalog.pg_policy definition

-- Drop table

-- DROP TABLE pg_catalog.pg_policy;

CREATE TABLE pg_catalog.pg_policy (
	"oid" oid NOT NULL,
	polname name COLLATE "C" NOT NULL,
	polrelid oid NOT NULL,
	polcmd char NOT NULL,
	polpermissive bool NOT NULL,
	polroles _oid NOT NULL,
	polqual pg_node_tree COLLATE "C" NULL,
	polwithcheck pg_node_tree COLLATE "C" NULL,
	CONSTRAINT pg_policy_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_policy_polrelid_polname_index UNIQUE (polrelid, polname)
);


-- pg_catalog.pg_proc definition

-- Drop table

-- DROP TABLE pg_catalog.pg_proc;

CREATE TABLE pg_catalog.pg_proc (
	"oid" oid NOT NULL,
	proname name COLLATE "C" NOT NULL,
	pronamespace oid NOT NULL,
	proowner oid NOT NULL,
	prolang oid NOT NULL,
	procost float4 NOT NULL,
	prorows float4 NOT NULL,
	provariadic oid NOT NULL,
	prosupport regproc NOT NULL,
	prokind char NOT NULL,
	prosecdef bool NOT NULL,
	proleakproof bool NOT NULL,
	proisstrict bool NOT NULL,
	proretset bool NOT NULL,
	provolatile char NOT NULL,
	proparallel char NOT NULL,
	pronargs int2 NOT NULL,
	pronargdefaults int2 NOT NULL,
	prorettype oid NOT NULL,
	proargtypes oidvector NOT NULL,
	proallargtypes _oid NULL,
	proargmodes _char NULL,
	proargnames _text COLLATE "C" NULL,
	proargdefaults pg_node_tree COLLATE "C" NULL,
	protrftypes _oid NULL,
	prosrc text COLLATE "C" NOT NULL,
	probin text COLLATE "C" NULL,
	prosqlbody pg_node_tree COLLATE "C" NULL,
	proconfig _text COLLATE "C" NULL,
	proacl _aclitem NULL,
	CONSTRAINT pg_proc_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_proc_proname_args_nsp_index UNIQUE (proname, proargtypes, pronamespace)
);


-- pg_catalog.pg_publication definition

-- Drop table

-- DROP TABLE pg_catalog.pg_publication;

CREATE TABLE pg_catalog.pg_publication (
	"oid" oid NOT NULL,
	pubname name COLLATE "C" NOT NULL,
	pubowner oid NOT NULL,
	puballtables bool NOT NULL,
	pubinsert bool NOT NULL,
	pubupdate bool NOT NULL,
	pubdelete bool NOT NULL,
	pubtruncate bool NOT NULL,
	pubviaroot bool NOT NULL,
	CONSTRAINT pg_publication_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_publication_pubname_index UNIQUE (pubname)
);


-- pg_catalog.pg_publication_rel definition

-- Drop table

-- DROP TABLE pg_catalog.pg_publication_rel;

CREATE TABLE pg_catalog.pg_publication_rel (
	"oid" oid NOT NULL,
	prpubid oid NOT NULL,
	prrelid oid NOT NULL,
	CONSTRAINT pg_publication_rel_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_publication_rel_prrelid_prpubid_index UNIQUE (prrelid, prpubid)
);


-- pg_catalog.pg_range definition

-- Drop table

-- DROP TABLE pg_catalog.pg_range;

CREATE TABLE pg_catalog.pg_range (
	rngtypid oid NOT NULL,
	rngsubtype oid NOT NULL,
	rngmultitypid oid NOT NULL,
	rngcollation oid NOT NULL,
	rngsubopc oid NOT NULL,
	rngcanonical regproc NOT NULL,
	rngsubdiff regproc NOT NULL,
	CONSTRAINT pg_range_rngmultitypid_index UNIQUE (rngmultitypid),
	CONSTRAINT pg_range_rngtypid_index PRIMARY KEY (rngtypid)
);


-- pg_catalog.pg_replication_origin definition

-- Drop table

-- DROP TABLE pg_catalog.pg_replication_origin;

CREATE TABLE pg_catalog.pg_replication_origin (
	roident oid NOT NULL,
	roname text COLLATE "C" NOT NULL,
	CONSTRAINT pg_replication_origin_roiident_index PRIMARY KEY (roident),
	CONSTRAINT pg_replication_origin_roname_index UNIQUE (roname)
)
TABLESPACE pg_global
;


-- pg_catalog.pg_rewrite definition

-- Drop table

-- DROP TABLE pg_catalog.pg_rewrite;

CREATE TABLE pg_catalog.pg_rewrite (
	"oid" oid NOT NULL,
	rulename name COLLATE "C" NOT NULL,
	ev_class oid NOT NULL,
	ev_type char NOT NULL,
	ev_enabled char NOT NULL,
	is_instead bool NOT NULL,
	ev_qual pg_node_tree COLLATE "C" NOT NULL,
	ev_action pg_node_tree COLLATE "C" NOT NULL,
	CONSTRAINT pg_rewrite_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_rewrite_rel_rulename_index UNIQUE (ev_class, rulename)
);


-- pg_catalog.pg_seclabel definition

-- Drop table

-- DROP TABLE pg_catalog.pg_seclabel;

CREATE TABLE pg_catalog.pg_seclabel (
	objoid oid NOT NULL,
	classoid oid NOT NULL,
	objsubid int4 NOT NULL,
	provider text COLLATE "C" NOT NULL,
	"label" text COLLATE "C" NOT NULL,
	CONSTRAINT pg_seclabel_object_index PRIMARY KEY (objoid, classoid, objsubid, provider)
);


-- pg_catalog.pg_sequence definition

-- Drop table

-- DROP TABLE pg_catalog.pg_sequence;

CREATE TABLE pg_catalog.pg_sequence (
	seqrelid oid NOT NULL,
	seqtypid oid NOT NULL,
	seqstart int8 NOT NULL,
	seqincrement int8 NOT NULL,
	seqmax int8 NOT NULL,
	seqmin int8 NOT NULL,
	seqcache int8 NOT NULL,
	seqcycle bool NOT NULL,
	CONSTRAINT pg_sequence_seqrelid_index PRIMARY KEY (seqrelid)
);


-- pg_catalog.pg_shdepend definition

-- Drop table

-- DROP TABLE pg_catalog.pg_shdepend;

CREATE TABLE pg_catalog.pg_shdepend (
	dbid oid NOT NULL,
	classid oid NOT NULL,
	objid oid NOT NULL,
	objsubid int4 NOT NULL,
	refclassid oid NOT NULL,
	refobjid oid NOT NULL,
	deptype char NOT NULL
)
TABLESPACE pg_global
;
CREATE INDEX pg_shdepend_depender_index ON pg_catalog.pg_shdepend USING btree (dbid, classid, objid, objsubid);
CREATE INDEX pg_shdepend_reference_index ON pg_catalog.pg_shdepend USING btree (refclassid, refobjid);


-- pg_catalog.pg_shdescription definition

-- Drop table

-- DROP TABLE pg_catalog.pg_shdescription;

CREATE TABLE pg_catalog.pg_shdescription (
	objoid oid NOT NULL,
	classoid oid NOT NULL,
	description text COLLATE "C" NOT NULL,
	CONSTRAINT pg_shdescription_o_c_index PRIMARY KEY (objoid, classoid)
)
TABLESPACE pg_global
;


-- pg_catalog.pg_shseclabel definition

-- Drop table

-- DROP TABLE pg_catalog.pg_shseclabel;

CREATE TABLE pg_catalog.pg_shseclabel (
	objoid oid NOT NULL,
	classoid oid NOT NULL,
	provider text COLLATE "C" NOT NULL,
	"label" text COLLATE "C" NOT NULL,
	CONSTRAINT pg_shseclabel_object_index PRIMARY KEY (objoid, classoid, provider)
)
TABLESPACE pg_global
;


-- pg_catalog.pg_statistic definition

-- Drop table

-- DROP TABLE pg_catalog.pg_statistic;

CREATE TABLE pg_catalog.pg_statistic (
	starelid oid NOT NULL,
	staattnum int2 NOT NULL,
	stainherit bool NOT NULL,
	stanullfrac float4 NOT NULL,
	stawidth int4 NOT NULL,
	stadistinct float4 NOT NULL,
	stakind1 int2 NOT NULL,
	stakind2 int2 NOT NULL,
	stakind3 int2 NOT NULL,
	stakind4 int2 NOT NULL,
	stakind5 int2 NOT NULL,
	staop1 oid NOT NULL,
	staop2 oid NOT NULL,
	staop3 oid NOT NULL,
	staop4 oid NOT NULL,
	staop5 oid NOT NULL,
	stacoll1 oid NOT NULL,
	stacoll2 oid NOT NULL,
	stacoll3 oid NOT NULL,
	stacoll4 oid NOT NULL,
	stacoll5 oid NOT NULL,
	stanumbers1 _float4 NULL,
	stanumbers2 _float4 NULL,
	stanumbers3 _float4 NULL,
	stanumbers4 _float4 NULL,
	stanumbers5 _float4 NULL,
	stavalues1 anyarray NULL,
	stavalues2 anyarray NULL,
	stavalues3 anyarray NULL,
	stavalues4 anyarray NULL,
	stavalues5 anyarray NULL,
	CONSTRAINT pg_statistic_relid_att_inh_index PRIMARY KEY (starelid, staattnum, stainherit)
);


-- pg_catalog.pg_statistic_ext definition

-- Drop table

-- DROP TABLE pg_catalog.pg_statistic_ext;

CREATE TABLE pg_catalog.pg_statistic_ext (
	"oid" oid NOT NULL,
	stxrelid oid NOT NULL,
	stxname name COLLATE "C" NOT NULL,
	stxnamespace oid NOT NULL,
	stxowner oid NOT NULL,
	stxstattarget int4 NOT NULL,
	stxkeys int2vector NOT NULL,
	stxkind _char NOT NULL,
	stxexprs pg_node_tree COLLATE "C" NULL,
	CONSTRAINT pg_statistic_ext_name_index UNIQUE (stxname, stxnamespace),
	CONSTRAINT pg_statistic_ext_oid_index PRIMARY KEY (oid)
);
CREATE INDEX pg_statistic_ext_relid_index ON pg_catalog.pg_statistic_ext USING btree (stxrelid);


-- pg_catalog.pg_statistic_ext_data definition

-- Drop table

-- DROP TABLE pg_catalog.pg_statistic_ext_data;

CREATE TABLE pg_catalog.pg_statistic_ext_data (
	stxoid oid NOT NULL,
	stxdndistinct pg_ndistinct COLLATE "C" NULL,
	stxddependencies pg_dependencies COLLATE "C" NULL,
	stxdmcv pg_mcv_list COLLATE "C" NULL,
	stxdexpr _pg_statistic NULL,
	CONSTRAINT pg_statistic_ext_data_stxoid_index PRIMARY KEY (stxoid)
);


-- pg_catalog.pg_subscription definition

-- Drop table

-- DROP TABLE pg_catalog.pg_subscription;

CREATE TABLE pg_catalog.pg_subscription (
	"oid" oid NOT NULL,
	subdbid oid NOT NULL,
	subname name COLLATE "C" NOT NULL,
	subowner oid NOT NULL,
	subenabled bool NOT NULL,
	subbinary bool NOT NULL,
	substream bool NOT NULL,
	subconninfo text COLLATE "C" NOT NULL,
	subslotname name COLLATE "C" NULL,
	subsynccommit text COLLATE "C" NOT NULL,
	subpublications _text COLLATE "C" NOT NULL,
	CONSTRAINT pg_subscription_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_subscription_subname_index UNIQUE (subdbid, subname)
)
TABLESPACE pg_global
;


-- pg_catalog.pg_subscription_rel definition

-- Drop table

-- DROP TABLE pg_catalog.pg_subscription_rel;

CREATE TABLE pg_catalog.pg_subscription_rel (
	srsubid oid NOT NULL,
	srrelid oid NOT NULL,
	srsubstate char NOT NULL,
	srsublsn pg_lsn NULL,
	CONSTRAINT pg_subscription_rel_srrelid_srsubid_index PRIMARY KEY (srrelid, srsubid)
);


-- pg_catalog.pg_tablespace definition

-- Drop table

-- DROP TABLE pg_catalog.pg_tablespace;

CREATE TABLE pg_catalog.pg_tablespace (
	"oid" oid NOT NULL,
	spcname name COLLATE "C" NOT NULL,
	spcowner oid NOT NULL,
	spcacl _aclitem NULL,
	spcoptions _text COLLATE "C" NULL,
	CONSTRAINT pg_tablespace_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_tablespace_spcname_index UNIQUE (spcname)
)
TABLESPACE pg_global
;


-- pg_catalog.pg_transform definition

-- Drop table

-- DROP TABLE pg_catalog.pg_transform;

CREATE TABLE pg_catalog.pg_transform (
	"oid" oid NOT NULL,
	trftype oid NOT NULL,
	trflang oid NOT NULL,
	trffromsql regproc NOT NULL,
	trftosql regproc NOT NULL,
	CONSTRAINT pg_transform_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_transform_type_lang_index UNIQUE (trftype, trflang)
);


-- pg_catalog.pg_trigger definition

-- Drop table

-- DROP TABLE pg_catalog.pg_trigger;

CREATE TABLE pg_catalog.pg_trigger (
	"oid" oid NOT NULL,
	tgrelid oid NOT NULL,
	tgparentid oid NOT NULL,
	tgname name COLLATE "C" NOT NULL,
	tgfoid oid NOT NULL,
	tgtype int2 NOT NULL,
	tgenabled char NOT NULL,
	tgisinternal bool NOT NULL,
	tgconstrrelid oid NOT NULL,
	tgconstrindid oid NOT NULL,
	tgconstraint oid NOT NULL,
	tgdeferrable bool NOT NULL,
	tginitdeferred bool NOT NULL,
	tgnargs int2 NOT NULL,
	tgattr int2vector NOT NULL,
	tgargs bytea NOT NULL,
	tgqual pg_node_tree COLLATE "C" NULL,
	tgoldtable name COLLATE "C" NULL,
	tgnewtable name COLLATE "C" NULL,
	CONSTRAINT pg_trigger_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_trigger_tgrelid_tgname_index UNIQUE (tgrelid, tgname)
);
CREATE INDEX pg_trigger_tgconstraint_index ON pg_catalog.pg_trigger USING btree (tgconstraint);


-- pg_catalog.pg_ts_config definition

-- Drop table

-- DROP TABLE pg_catalog.pg_ts_config;

CREATE TABLE pg_catalog.pg_ts_config (
	"oid" oid NOT NULL,
	cfgname name COLLATE "C" NOT NULL,
	cfgnamespace oid NOT NULL,
	cfgowner oid NOT NULL,
	cfgparser oid NOT NULL,
	CONSTRAINT pg_ts_config_cfgname_index UNIQUE (cfgname, cfgnamespace),
	CONSTRAINT pg_ts_config_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_ts_config_map definition

-- Drop table

-- DROP TABLE pg_catalog.pg_ts_config_map;

CREATE TABLE pg_catalog.pg_ts_config_map (
	mapcfg oid NOT NULL,
	maptokentype int4 NOT NULL,
	mapseqno int4 NOT NULL,
	mapdict oid NOT NULL,
	CONSTRAINT pg_ts_config_map_index PRIMARY KEY (mapcfg, maptokentype, mapseqno)
);


-- pg_catalog.pg_ts_dict definition

-- Drop table

-- DROP TABLE pg_catalog.pg_ts_dict;

CREATE TABLE pg_catalog.pg_ts_dict (
	"oid" oid NOT NULL,
	dictname name COLLATE "C" NOT NULL,
	dictnamespace oid NOT NULL,
	dictowner oid NOT NULL,
	dicttemplate oid NOT NULL,
	dictinitoption text COLLATE "C" NULL,
	CONSTRAINT pg_ts_dict_dictname_index UNIQUE (dictname, dictnamespace),
	CONSTRAINT pg_ts_dict_oid_index PRIMARY KEY (oid)
);


-- pg_catalog.pg_ts_parser definition

-- Drop table

-- DROP TABLE pg_catalog.pg_ts_parser;

CREATE TABLE pg_catalog.pg_ts_parser (
	"oid" oid NOT NULL,
	prsname name COLLATE "C" NOT NULL,
	prsnamespace oid NOT NULL,
	prsstart regproc NOT NULL,
	prstoken regproc NOT NULL,
	prsend regproc NOT NULL,
	prsheadline regproc NOT NULL,
	prslextype regproc NOT NULL,
	CONSTRAINT pg_ts_parser_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_ts_parser_prsname_index UNIQUE (prsname, prsnamespace)
);


-- pg_catalog.pg_ts_template definition

-- Drop table

-- DROP TABLE pg_catalog.pg_ts_template;

CREATE TABLE pg_catalog.pg_ts_template (
	"oid" oid NOT NULL,
	tmplname name COLLATE "C" NOT NULL,
	tmplnamespace oid NOT NULL,
	tmplinit regproc NOT NULL,
	tmpllexize regproc NOT NULL,
	CONSTRAINT pg_ts_template_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_ts_template_tmplname_index UNIQUE (tmplname, tmplnamespace)
);


-- pg_catalog.pg_type definition

-- Drop table

-- DROP TABLE pg_catalog.pg_type;

CREATE TABLE pg_catalog.pg_type (
	"oid" oid NOT NULL,
	typname name COLLATE "C" NOT NULL,
	typnamespace oid NOT NULL,
	typowner oid NOT NULL,
	typlen int2 NOT NULL,
	typbyval bool NOT NULL,
	typtype char NOT NULL,
	typcategory char NOT NULL,
	typispreferred bool NOT NULL,
	typisdefined bool NOT NULL,
	typdelim char NOT NULL,
	typrelid oid NOT NULL,
	typsubscript regproc NOT NULL,
	typelem oid NOT NULL,
	typarray oid NOT NULL,
	typinput regproc NOT NULL,
	typoutput regproc NOT NULL,
	typreceive regproc NOT NULL,
	typsend regproc NOT NULL,
	typmodin regproc NOT NULL,
	typmodout regproc NOT NULL,
	typanalyze regproc NOT NULL,
	typalign char NOT NULL,
	typstorage char NOT NULL,
	typnotnull bool NOT NULL,
	typbasetype oid NOT NULL,
	typtypmod int4 NOT NULL,
	typndims int4 NOT NULL,
	typcollation oid NOT NULL,
	typdefaultbin pg_node_tree COLLATE "C" NULL,
	typdefault text COLLATE "C" NULL,
	typacl _aclitem NULL,
	CONSTRAINT pg_type_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_type_typname_nsp_index UNIQUE (typname, typnamespace)
);


-- pg_catalog.pg_user_mapping definition

-- Drop table

-- DROP TABLE pg_catalog.pg_user_mapping;

CREATE TABLE pg_catalog.pg_user_mapping (
	"oid" oid NOT NULL,
	umuser oid NOT NULL,
	umserver oid NOT NULL,
	umoptions _text COLLATE "C" NULL,
	CONSTRAINT pg_user_mapping_oid_index PRIMARY KEY (oid),
	CONSTRAINT pg_user_mapping_user_server_index UNIQUE (umuser, umserver)
);