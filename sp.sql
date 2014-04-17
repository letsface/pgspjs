SELECT 
    proname,    
    (SELECT array_to_json(ARRAY(
        SELECT pg_type.typname 
        FROM unnest(proargtypes) AS pat, pg_type
        WHERE pat = pg_type.oid))
    ) AS argstypes,
    array_to_json(proargnames) as argsnames,
    pg_type.typname AS rettype, 
    nspname AS namespace
FROM 
    pg_proc,
    pg_type, 
    pg_namespace
WHERE 
    pg_namespace.oid = pg_proc.pronamespace
    AND prorettype = pg_type.oid
    -- schema (public by default)
    AND (
        pg_namespace.nspname = 'public' 
        OR pg_namespace.nspname = $1  
    )
    