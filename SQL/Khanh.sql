select 		lea_state, leaid, 
			sum(cast(tot_mathenr_alg2_m as float)), 
			sum(cast(tot_mathenr_alg2_f as float)) 
from		"CRDB".algebraii
where		tot_mathenr_alg2_m <> '-9' 
			and tot_mathenr_alg2_f <> '-9'
group by	lea_state, leaid
limit 100;
			