{{ config(materialized='table') }}

with source_data as (

    select x.guid, c.start, (x.party ->> 'id')::int as "agentId", replace(c.duration,',','')::numeric  as totalDuration,x."endReason",x.events, (x.events->0->>'duration')::int as ringing, (x.events->1->>'duration')::int as connected, (x.events->3->>'duration')::int as wrap
	from {{ source('calls', 'callsraw') }} c, json_to_recordset(channels::json) x 
        (  guid text
         , party json
         , events json
         , "offset" integer
         , duration integer
         , "endReason" text
		 , "integrationType" text
        )

)


SELECT  *
FROM source_data

