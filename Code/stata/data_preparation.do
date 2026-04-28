** Path

global raw="Your path/Stranded_Occupations_Replication/Data/raw"
global temp="Your path/Stranded_Occupations_Replication/Data/temp"
global use="Your path/Stranded_Occupations_Replication/Data/use"

cd "$raw"



/*Note: Execute data_preparation first, followed by data_analysis, in the prescribed sequence of each script. There is no need to manage file storage manually, as datasets not required for preservation will be cleared automatically.*/


*** Data preparation

** Fig.1
**--------------------------------------------------------------------------------------

import excel "$raw/stranded industry_20250220.xlsx", sheet("fossil fuel industries") firstrow clear // Import the raw Excel file for the fossil fuel industry

tostring NAICSCode,gen( NAICS_2022) // Generate a string variable for the 6-digit NAICS fossil fuel industry codes, which may be required for subsequent character-based matching
 
rename NAICSCode NAICS2022 // Standardize the variable name for the 6-digit NAICS fossil fuel industry codes
 
gen strand_ind=1 // Assign a value of 1 to indicate fossil fuel industries

save "$temp/naics_strand.dta", replace // 6-digit NAICS fossil fuel industries


import excel "$raw/occupation_information.xlsx",  firstrow clear // Import the dataset containing fundamental information for all occupations, including major categories and classification types, with a total of 1,016 occupations (covering both 6-digit and 8-digit occupational codes)

drop if type_skill_all=="Military"  // Following Cortes (2012), occupations related to the military are classified under the occupational type "Military" and are excluded from the analytical sample.

rename Title Occupationtitle // Standardize the variable name for occupations

sort occupation_code 
// First, sort by occupation_code so that subsequent duplicates drop retains only the first occurrence of each 6-digit occupation.  
// For example, after sorting: 113051 (Industrial Production Managers) followed by 113051.03 (Quality Control Systems Managers),  
// duplicates drop will keep 113051 (Industrial Production Managers) and remove 113051.03 (Quality Control Systems Managers).


replace occupation_code = int(occupation_code) 
// Transform decimal occupation codes into their corresponding integer 6-digit codes to facilitate later matching.  
// For instance, 113051.03 (Quality Control Systems Managers) is mapped to 113051 (Industrial Production Managers),  
// since 113051.03 is a sub-occupation of the broader 113051 occupation.

duplicates drop occupation_code,force 
// Following the conversion of decimal occupation codes to their corresponding 6-digit integers, certain codes are duplicated (e.g., 113051.03 → 113051). Ultimately, 848 unique 6-digit occupations are preserved.

gen match_Occupationtitle= Occupationtitle // Generate a variable for matched occupation title, which will be used for linking and matching occupation-related information in subsequent steps

gen occupation_code_match=occupation_code //Generate a variable representing the matched occupation code, which will be used for linking and matching occupation-related information in subsequent steps

save "$temp/occupation_information.dta", replace // A total of 848 unique 6-digit occupations are retained after excluding Military occupations


import excel "$raw/naics_nem_new.xlsx", sheet("Sheet1") firstrow clear
// Import the dataset linking 6-digit NAICS industries (6-digit) to occupations (4-, 5-, and 6-digit).

/* The dataset has been preprocessed as follows: First, the original data's NEM industry codes were matched to 6-digit NAICS codes using the file nem-industry-coverage.xlsx.  
Since the raw industry-occupation data provided by BLS mostly include only 4- and 5-digit NAICS industry-occupation information, the 6-digit NAICS codes were mapped to the nearest 4- or 5-digit codes.

The mapping between NEM industries and 6-digit NAICS codes is available in $raw/NAICS_nem_code.xlsx.  
The procedure for matching 6-digit NAICS codes with occupation codes is documented in 
/Stranded_Occupations_Replication/Code/python/nem_naics_match.ipynb.

Note that the original BLS data do not provide mapping information for 999200,300 (State and local government, excluding education and hospitals) to NAICS 2022.  
Consequently, this analysis does not include the "State and local government, excluding education and hospitals" industry.
*/


gen occupation_code=subinstr(Occupationcode, "-", "", .) // Example: transform the 6-digit code 11-3051 into 113051 to enable its later use as a numeric variable

destring occupation_code,replace  

gen  occupation_code_match=occupation_code //Generate a variable representing the matched occupation code, which will be used for linking and matching occupation-related information in subsequent steps

save "$temp/naics_nem_new", replace 
 
merge m:1 NAICS2022 using  "$temp/naics_strand.dta",keepusing(strand_ind) //Identify fossil fuel industries
drop if _merge==2
drop _merge  

replace strand_ind=0 if strand_ind==. //Identify non-fossil fuel industries

merge m:1 occupation_code using "$temp/occupation_information.dta",keepusing(type_skill_all) // Identify occupational type

drop if _merge==2
drop _merge 
// Note: "$temp/occupation_information.dta" contains 6-digit occupation information,  
// whereas "$temp/naics_nem_new.dta" includes industry-occupation data at the 4- and 5-digit level.  
// For example, 111000 (Top executives) encompasses 111011 (Chief executives) and 111021 (General and operations managers).  
// Since 111000 is not the most granular 6-digit occupation code, only 111011 and 111021 are considered specific occupations.  
// Therefore, 111000 cannot be matched to skill information.


gen nrm=1 if type_skill_all=="Non-Routine Manual"
gen rm=1 if type_skill_all=="Routine Manual"
gen nrc=1 if type_skill_all=="Non-Routine Cognitive"
gen rc=1 if type_skill_all=="Routine Cognitive"


save "$temp/naics_nem_new.dta", replace 
  
use "$temp/naics_nem_new",clear

keep if strand_ind==1  //Keep only fossil fuel industries

duplicates drop Occupationcode,force // Obtain unique 4- to 6-digit occupations in fossil fuel industries (hereafter referred to as "fossil fuel occupations")

drop if type_skill_all=="" 
// Note: "$temp/occupation_information.dta" contains 6-digit occupation information,  
// whereas "$temp/naics_nem_new.dta" includes industry-occupation data at the 4- and 5-digit levels.  
// For example, 111000 (Top executives) encompasses 111011 (Chief executives) and 111021 (General and operations managers).  
// Since 111000 is not the most detailed 6-digit occupation code, only 111011 and 111021 represent specific occupations.  
// Consequently, 111000 cannot be matched to skill information.  
// We removed these 4- and 5-digit occupations, retaining only 6-digit occupations.


drop if occupation_code==472231 | occupation_code==194051 | occupation_code==172161 | occupation_code==499081
// since the BLS report some occupation–industry linkages at the 4- or 5-digit level, the 6-digit fossil fuel industries were mapped to the nearest 4- or 5-digit NAICS categories.This broader mapping unintentionally incorporated a few clean-energy occupations—such as Solar Photovoltaic Installers, Nuclear Technicians, Nuclear Engineers, and Wind Turbine Service Technicians—which are not fossil fuel–related. To ensure that the identified fossil fuel occupations accurately reflect conventional fossil energy activities, these occupations were excluded from the analysis.

// A total of 399 unique 6-digit fossil fuel occupations remain.

gen strandind_new_ocu=1 

save "$temp/strandind_new_ocu",replace //six-digit fossil fuel occupations

use "$temp/naics_nem_new",clear
drop if strand_ind==1 // Remove fossil fuel industries
drop if type_skill_all=="" 
duplicates drop Occupationcode,force // Unique 6-digit occupations occupations in non-fossil fuel industries
gen nostrandind_new_ocu=1
save "$temp/nostrandind_new_ocu",replace //806 six-digit non-fossil fuel occupations

use "$temp/occupation_information.dta",clear 

merge m:1 occupation_code using "$temp/strandind_new_ocu.dta",keepusing(strandind_new_ocu) // Match the 6-digit fossil fuel occupations to the full set of occupation information

drop if _merge==2
drop _merge 

merge m:1 occupation_code using "$temp/nostrandind_new_ocu.dta",keepusing(nostrandind_new_ocu) // Match the 6-digit non-fossil fuel occupations to the full set of occupation information


drop if _merge==2
drop _merge 


save "$temp/occupation_information.dta",replace 
*******Fig.1C*******
// Fig.1C left
use "$temp/naics_nem_new.dta", clear

merge m:1 occupation_code using "$temp/strandind_new_ocu.dta",keepusing(strandind_new_ocu) // Identify whether fossil fuel occupations exist within each industry

drop if _merge==2
drop _merge 

drop if type_skill_all=="" 
// Note: "$temp/occupation_information.dta" contains 6-digit occupation information,  
// whereas "$temp/naics_nem_new.dta" includes industry-occupation records at the 4- and 5-digit levels.  
// For example, 111000 (Top executives) encompasses 111011 (Chief executives) and 111021 (General and operations managers).  
// Since 111000 is not the most detailed 6-digit occupation code, only 111011 and 111021 represent specific occupations.  
// Therefore, 111000 cannot be matched to skill information.  
// We removed these 4- and 5-digit industry-occupation records, retaining only 6-digit industry-occupation data.

keep if strand_ind == 0 // Non-fossil fuel industries
keep if strandind_new_ocu == 1 // Retain fossil fuel occupations
collapse (count) NAICS2022, by(occupation_code) // Calculate the frequency of fossil fuel occupations appearing in non-fossil fuel industries
 rename NAICS2022 app_nofossil
save "$temp/occ_num_nostr",replace //The frequency of fossil fuel occupations appearing in non-fossil fuel industries

use "$temp/strandind_new_ocu",clear 
merge m:1 occupation_code using "$temp/occ_num_nostr",keepusing(app_nofossil) 
drop if _merge==2
drop _merge 

gen proportion_nostr= app_nofossil/958  //The frequency of an occupation occurrence in non-fossil fuel industries divided by the total number of those industries (n=958)

/* There are 958 unique 6-digit non-fossil fuel industries and 25 fossil fuel industries
use "$temp/naics_nem_new.dta", clear
drop if type_skill_all=="" // Remove occupations without 6-digit codes
duplicates drop NAICS2022, force // Resulting in a total of 983 unique 6-digit NAICS 2022 industries
*/
 
replace proportion_nostr = 0 if proportion_nostr == .  
// (1 real change made) Roof Bolters, Mining did not appear in other industries, so proportion_nostr is set to 0

save "$temp/strandind_new_ocu",replace 


// Fig.1C right
use "$temp/naics_nem_new.dta", clear

drop if type_skill_all=="" // Remove non-6-digit occupations

save "$temp/naics_nem_new_em.dta", replace

collapse (sum) Employment, by ( occupation_code Occupationtitle strand_ind) // Calculate the total employment of each occupation separately within fossil fuel and non-fossil fuel industries

egen employment_all= sum (Employment),by (occupation_code) // Compute the total number of employees for each occupation

drop if strand_ind==0 // Exclude the number of employees for each occupation within non-fossil fuel industries

gen strand_emrate=Employment/employment_all // Calculate the share of an occupation's national workforce employed in fossil fuel industries

save "$temp/Employment_by",replace

use "$temp/strandind_new_ocu",clear  

merge m:1 occupation_code using "$temp/Employment_by",keepusing(strand_emrate) //The share of an occupation's national workforce employed in fossil fuel industries

drop if _merge==2
drop _merge 

save "$temp/strandind_new_ocu",replace



** Fig.2
**--------------------------------------------------------------------------------------

//Fig.2A

import delimited "$use/panel_jskill_sim.csv", varnames(1) clear
rename occupation Occupationtitle
rename match_occupation match_Occupationtitle

merge m:1 Occupationtitle using "$temp/occupation_information.dta", keepusing( occupation_code type_skill_all) // Match the 6-digit occupation codes; 8-digit occupation codes are not matched because occupation_information.dta only contains 6-digit occupation data
drop if _merge==2
drop _merge 

merge m:1 match_Occupationtitle using "$temp/occupation_information.dta", keepusing( occupation_code_match match_type_skill) 
drop if _merge==2
drop _merge 

order match_type_skill, after(type_skill_all)

replace skill_similarity=1 if skill_similarity==. // NA values correspond to comparisons within the same occupation and are therefore set to 1

drop if type_skill_all=="" 
// Note: "$temp/occupation_information.dta" contains 6-digit occupation data,  
// whereas "$use/panel_jskill_sim.csv" also includes some 8-digit occupations, e.g., 111011.03 (General and Operations Managers).  
// Therefore, 111011.03 cannot be matched to skill information.  
// As this analysis primarily relies on 6-digit occupations, all 8-digit occupations and their matched occupations are removed.  
// Since the matrix is symmetric, the number of unmatched entries is the same for OccupationTitle and Match_OccupationTitle.

/*
merge m:1 Occupationtitle using "$temp/occupation_information.dta"

    Result                      Number of obs
    -----------------------------------------
    Not matched                       130,643
        from master                   130,524  (_merge==1)
        from using                        119  (_merge==2)

    Matched                           668,712  (_merge==3)
    -----------------------------------------
	
merge m:1 match_Occupationtitle using "$temp/occupation_information.dta", keepusing( occupation_code_match match_type_skill)

    Result                      Number of obs
    -----------------------------------------
    Not matched                       130,643
        from master                   130,524  (_merge==1)
        from using                        119  (_merge==2)

    Matched                           668,712  (_merge==3)
    -----------------------------------------

*/

drop if match_type_skill=="" 
save "$temp/jskill_pa",replace // Similarity between 6-digit occupations and 6-digit matched occupations


use "$temp/jskill_pa.dta", clear
merge m:1 occupation_code using "$temp/strandind_new_ocu.dta",keepusing( strandind_new_ocu) // Identify fossil fuel occupations

drop if _merge==2
drop _merge 

merge m:1 occupation_code using "$temp/nostrandind_new_ocu.dta",keepusing( nostrandind_new_ocu) // Identify non-fossil fuel occupations

drop if _merge==2
drop _merge 

save "$temp/jskill_pa.dta",replace 

export delimited using "/Users/huangqing/Documents/paper/strand_labor/Stranded_Occupations_Replication/Data/temp/jskill_pa.csv", replace //for further analysis


// Fig.2b 
use "$temp/jskill_pa.dta", clear
keep if  strandind_new_ocu==1 
keep if skill_similarity>0.85| skill_similarity==0.85 //Fossil fuel occupations with high transferability
drop if skill_similarity==1

save "$temp/simocc_851", replace

 use "$temp/simocc_851", replace

duplicates drop occupation_code,force

gen simocc_851_uq=1
save "$temp/simocc_851_uq", replace


use "$temp/strandind_new_ocu",clear  

merge m:1 occupation_code using "$temp/simocc_851_uq",keepusing(simocc_851_uq)
drop if _merge==2
drop _merge 
replace simocc_851_uq=0 if simocc_851_uq==.
save "$temp/strandind_new_ocu",replace

use "$temp/jskill_pa.dta", clear
keep if  strandind_new_ocu==1 
keep if skill_similarity<0.85&skill_similarity>=0.8 //Fossil fuel occupations with high transferability after reskilling

save "$temp/simocc_885", replace

 use "$temp/simocc_885", replace


duplicates drop occupation_code,force

gen simocc_885_uq=1
save "$temp/simocc_885_uq", replace

use "$temp/strandind_new_ocu",clear  

merge m:1 occupation_code using "$temp/simocc_885_uq",keepusing(simocc_885_uq)
drop if _merge==2
drop _merge 
replace simocc_885_uq=0 if simocc_885_uq==. 
replace simocc_885_uq=2 if simocc_851_uq==1
 // simocc_89_uq: 0 indicates low transferability, 1 indicates high transferability after reskilling, and 2 indicates high transferability
save "$temp/strandind_new_ocu",replace

export excel using "$use/simocc885&851_uq.xlsx", firstrow(varlabels) replace // Create Figure 2b based on the exported Excel dataset


// Fig. 2c left & middle

use "$temp/jskill_pa.dta", clear
keep if skill_similarity>=0.8 // Occupation pairs with similarity ≥ 0.85 and < 1
keep if  strandind_new_ocu==1 
  drop if skill_similarity==1
  
save "$temp/simocc_81", replace


use "$temp/simocc_851", clear
drop if occupation_code_match==475043 
// Calculate transitions from fossil fuel occupations to non-fossil fuel occupations by occupation type.  
// Roof Bolters, Mining is excluded from matched occupations because they do not exist in non-fossil fuel industries.

 gen k=_n  // Generate a numerical value for each occupation pair

collapse (count) k ,by (type_skill_all match_type_skill) 

 save "$temp/fig2cl",replace 
export excel using "$use/fig2cl.xlsx", firstrow(varlabels) replace 

use "$temp/simocc_81", clear
drop if occupation_code_match==475043 //Roof Bolters, Mining is excluded from matched occupations

 gen k=_n // Generate a numerical value for each occupation pair

collapse (count) k ,by (type_skill_all match_type_skill) 

 save "$temp/fig2cr",replace 
 export excel using "$use/fig2cr.xlsx", firstrow(varlabels) replace //extract excel for sankey chart
 

 //skill_similarity==1 
 use "$temp/jskill_pa.dta", clear

keep if skill_similarity==1 
// Retain occupation pairs with similarity equal to 1, resulting in 748 pairs, which is fewer than the 848 fossil fuel occupations.  
// The main reason is that ONET provides skill information for 748 6-digit occupations, whereas occupation_information contains 848 6-digit occupations.  
// Consequently, the number of 6-digit occupations with skill similarity is smaller than the total number of fossil fuel occupations identified in this study.

keep if  strandind_new_ocu==1 
// Retain fossil fuel occupation pairs with similarity equal to 1, resulting in 357 pairs

save "$temp/simocc_1", replace 


use "$temp/simocc_1", clear
 gen nrm=1 if type_skill_all=="Non-Routine Manual"
 gen rm=1 if type_skill_all=="Routine Manual"
 gen nrc=1 if type_skill_all=="Non-Routine Cognitive"
 gen rc=1 if type_skill_all=="Routine Cognitive"
 
save "$temp/simocc_1", replace


** Fig.3
**--------------------------------------------------------------------------------------
use "$temp/jskill_pa.dta", clear
keep if skill_similarity>=0.8
keep if  strandind_new_ocu==1 

save "$temp/simocc_8k1", replace


use "$temp/simocc_8k1", clear
 gen nrm=1 if type_skill_all=="Non-Routine Manual"
 gen rm=1 if type_skill_all=="Routine Manual"
 gen nrc=1 if type_skill_all=="Non-Routine Cognitive"
 gen rc=1 if type_skill_all=="Routine Cognitive"
 
save "$temp/simocc_8k1", replace

local var nrm rm nrc rc
foreach var in `var' {
	use "$temp/simocc_8k1",clear
	keep if `var'==1

save "$temp/simocc_8k1`var'",replace // Extract occupations by occupational type

}


local var nrm rm nrc rc
foreach var in `var' {
	use "$temp/simocc_8k1`var'",clear
duplicates drop occupation_code_match,force //Similar matched occupations corresponding to different stranded industry job categories
gen simocconly_8k1`var'=1
save "$temp/simocconly_8k1`var'",replace
}
 
local var nrm rm nrc rc
foreach var in `var' {
  use "$temp/naics_nem_new",clear 
  merge m:1 occupation_code_match using "$temp/simocconly_8k1`var'",keepusing(simocconly_8k1`var') // Matched occupations for fossil fuel occupations
drop if _merge==2
drop _merge 
save "$temp/naics_nem_new",replace
				
}

*========================
* 生成两位数行业前缀
* 例如 211120 -> 21
*========================
gen naics2 = floor(NAICS2022/10000)

*========================
* 生成匹配结果变量
*========================
gen naics_group = ""
replace naics_group = "11"    if naics2 == 11
replace naics_group = "21"    if naics2 == 21
replace naics_group = "22"    if naics2 == 22
replace naics_group = "23"    if naics2 == 23
replace naics_group = "31-33" if inrange(naics2, 31, 33)
replace naics_group = "42"    if naics2 == 42
replace naics_group = "44-45" if inrange(naics2, 44, 45)
replace naics_group = "48-49" if inrange(naics2, 48, 49)
replace naics_group = "51"    if naics2 == 51
replace naics_group = "52"    if naics2 == 52
replace naics_group = "53"    if naics2 == 53
replace naics_group = "54"    if naics2 == 54
replace naics_group = "55"    if naics2 == 55
replace naics_group = "56"    if naics2 == 56
replace naics_group = "61"    if naics2 == 61
replace naics_group = "62"    if naics2 == 62
replace naics_group = "71"    if naics2 == 71
replace naics_group = "72"    if naics2 == 72
replace naics_group = "81"    if naics2 == 81
replace naics_group = "92"    if naics2 == 92

*========================
* 生成官方英文名称变量
*========================
gen naics_name = ""
replace naics_name = "Agriculture, Forestry, Fishing and Hunting" ///
    if naics_group == "11"
	
replace naics_name = "Mining, Quarrying, and Oil and Gas Extraction" ///
    if naics_group == "21"

replace naics_name = "Utilities" ///
    if naics_group == "22"

replace naics_name = "Construction" ///
    if naics_group == "23"

replace naics_name = "Manufacturing" ///
    if naics_group == "31-33"

replace naics_name = "Wholesale Trade" ///
    if naics_group == "42"

replace naics_name = "Retail Trade" ///
    if naics_group == "44-45"

replace naics_name = "Transportation and Warehousing" ///
    if naics_group == "48-49"

replace naics_name = "Information" ///
    if naics_group == "51"

replace naics_name = "Finance and Insurance" ///
    if naics_group == "52"

replace naics_name = "Real Estate and Rental and Leasing" ///
    if naics_group == "53"

replace naics_name = "Professional, Scientific, and Technical Services" ///
    if naics_group == "54"

replace naics_name = "Management of Companies and Enterprises" ///
    if naics_group == "55"

replace naics_name = "Administrative and Support and Waste Management and Remediation Services" ///
    if naics_group == "56"

replace naics_name = "Educational Services" ///
    if naics_group == "61"

replace naics_name = "Health Care and Social Assistance" ///
    if naics_group == "62"

replace naics_name = "Arts, Entertainment, and Recreation" ///
    if naics_group == "71"

replace naics_name = "Accommodation and Food Services" ///
    if naics_group == "72"

replace naics_name = "Other Services (except Public Administration)" ///
    if naics_group == "81"

replace naics_name = "Public Administration" ///
    if naics_group == "92"
	
save "$temp/naics_nem_new",replace

	
local var nrm rm nrc rc
foreach var in `var' {
	use "$temp/naics_nem_new",clear
	egen sum_8k1`var'=sum (Employment) if strand_ind==0&simocconly_8k1`var'==1,by(naics_group naics_name) //Calculate the employment of similar occupations
	sort sum_8k1`var'
	duplicates drop  naics_group,force
save "$temp/sum_8k1`var'",replace //Employment size of similar matched occupations within occupational groups of different stranded industries


}


** Fig.4
**--------------------------------------------------------------------------------------
use "$temp/jskill_pa.dta", clear
keep if skill_similarity>=0.8
keep if  strandind_new_ocu==1 

save "$temp/simocc_08", replace

 export excel using "/Users/huangqing/Documents/paper/strand_labor/Stranded_Occupations_Replication/Data/temp/simocc_08.xlsx", firstrow(variables) replace

** Fig.5
**--------------------------------------------------------------------------------------
// Since BLS wage data are available only at the 4-digit industry-occupation level, it is necessary to align 4-digit industry codes with the 6-digit fossil fuel industries.


//fig5a
use "$temp/simocc_1",clear

 export excel using "/Users/huangqing/Documents/paper/strand_labor/Stranded_Occupations_Replication/Data/temp/simocc_1.xlsx", firstrow(variables) replace

//fig5b
 use "$temp/simocc_851", clear

   export excel using "/Users/huangqing/Documents/paper/strand_labor/Stranded_Occupations_Replication/Data/temp/simocc_851.xlsx", firstrow(variables) replace
  
 
 //fig5c
use "$temp/simocc_885", clear

 export excel using "/Users/huangqing/Documents/paper/strand_labor/Stranded_Occupations_Replication/Data/temp/simocc_885.xlsx", firstrow(variables) replace

** Supplement Fig.5 

use "$temp/jskill_pa.dta", clear
keep if skill_similarity>=0.8
keep if  strandind_new_ocu==1 

duplicates drop occupation_code_match,force

drop if occupation_code_match==475043 //Roof Bolters, Mining is excluded from matched occupations

save "$temp/occupation_code_match_81.dta", replace


 
 clear all

