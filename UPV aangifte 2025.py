# Databricks notebook source
#Jira ticket: DATAENG-1936

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,SETTINGS
INBOUND_START = '2025-01-01'
INBOUND_END = '2025-12-31'

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Material

# COMMAND ----------

# DBTITLE 1,Matrial property code filtering
property_codes_containing_materiaal = (
    spark.table("prod_wrg_deltalake.dl_gld.article_properties")
    .where((F.lower(F.col("property_desc")).contains("materiaal")) | F.lower(F.col("property_desc")).contains("material"))
    .select(F.col("property_code"))
    .distinct()
)
property_codes = [row['property_code'] for row in property_codes_containing_materiaal.collect()]
print(property_codes)



all_articles_with_property_codes = (
    spark.table('prod_wrg_deltalake.dl_gld.article_properties')
    .filter(
        (F.col("property_code").isin(property_codes))
    )
    .select(
        F.col("article_id"), 
        F.col("property_code"), 
        F.col("property_desc"), 
        F.lower("property_text").alias('property_text')
    )
    .withColumn("property_text", F.regexp_replace("property_text", "\.", ""))
    .withColumn("property_desc", F.regexp_replace("property_desc", "\.", ""))
    .withColumn("property_desc",  # <- EERST property_desc standaardiseren
        F.when(F.col("property_code") == "Y02", F.lit("materiaal"))
         .otherwise(F.col("property_desc"))
    )
    .withColumn('code_desc', F.concat(F.col('property_code'), F.lit(' - '), F.lower(F.col('property_desc'))))  # <- DAN pas code_desc maken
    .groupBy('article_id').pivot('code_desc').agg(F.first(F.col('property_text')).alias('property_text'))
    .distinct()
)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Summary: 
# MAGIC It looks like we have a lot of material columns, but there are 2 main columns: YO2 for specifics around percentages and QBC for general (the one that fits the most)
# MAGIC In order to get everything lined up, let's group all columns, except QBC, YO2. 
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Group material columsn, except for YO2 and QBC
exclude_cols = ['Y02 - materiaal','QBC - materiaal', 'article_id']
combined_cols = [i for i in all_articles_with_property_codes.columns if i not in exclude_cols]

print(exclude_cols)
print(combined_cols)

all_articles_with_property_codes_c = (all_articles_with_property_codes
                                      .select(*exclude_cols, F.array_compact(F.array_distinct(F.array(*combined_cols))).alias('list_other_material'))
                                      .withColumn('size_list_other', F.size(F.col('list_other_material')))
)

#100078

display(all_articles_with_property_codes_c
        .where(F.col('Y02 - materiaal').isNull())
        .where(F.col('QBC - materiaal').isNull())
        .groupBy('size_list_other').count()

)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Summary: 
# MAGIC We see only in ca 7k records that we have no QBC or YO2 field. In those records, most of them have only one other material. Let's fetch the first present, so we get what we need

# COMMAND ----------

# DBTITLE 1,Final_item_material
all_articles_with_property_codes_c_u = (all_articles_with_property_codes_c
                                        .groupBy('Y02 - materiaal','QBC - materiaal', 'article_id').agg(F.first('list_other_material').alias('other_material_field'))
                                        )

display(all_articles_with_property_codes_c_u)                                        

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Inbound

# COMMAND ----------

# DBTITLE 1,Inbound
inbound = (
        table('prod_wrg_deltalake.dl_gld_reporting.fascia_country_article_day_diverse')
        .where(F.col("date_date").between(INBOUND_START, INBOUND_END))
        .groupBy("article_id", "country_code")
        .agg(
            F.sum("actualised_sales_qty").alias("actualised_sales_qty"),
            F.sum("net_despatch_qty").alias("net_despatch_qty")
    )
    .join(
        table('prod_wrg_deltalake.dl_gld.articles')
        .select(
            "article_id",
            "article_desc",
            "brand_name",
            "brand_line_name",
            "preferred_supplier_name",
            "private_brand_ind",
            "business_model_desc",
            "mcc_1_name",
            "mcc_2_name",
            "mcc_3_name",
            "bss_1_name",
            "bss_2_name",
            "article_type_id"
        ),
        on="article_id", how="left"
    )
    .na.fill(0)
    .orderBy("article_id")
)

country_code = ["NL", "BE"]
inbound = (inbound.where(F.col("country_code").isin(country_code)))

#  Display the result
display(inbound)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Weight

# COMMAND ----------

# DBTITLE 1,Weight
cutoff_weight_in_grams = 250000
article_weight = (spark.read.table("prod_wrg_deltalake.dl_gld.articles")
           .select("article_id","preferred_supplier_id","article_gross_weight_num")
           .withColumn('article_gross_weight_num_corrected', F.when(F.col('article_gross_weight_num')>cutoff_weight_in_grams, cutoff_weight_in_grams).otherwise(F.col('article_gross_weight_num')))
           .distinct()
           .na.fill(0)
          )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Combine all 

# COMMAND ----------

# DBTITLE 1,Combined

inbound_material_weight = (inbound
                           .join(all_articles_with_property_codes_c_u, on='article_id', how='left')
                           .join(article_weight, on='article_id', how='left')
                           # .join(ean_codes, on='article_id', how='left')
                        #    .withColumn('other_material_field', F.explode_outer(F.col('other_material_field')))
                           )

display(inbound_material_weight)    
display(inbound_material_weight.count())

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
import re
import unicodedata  

# Define a list of materials to search for
materials_list = [
    "acetaat", "acryl", "alpaca", "angora", "bamboe", "canvas", 
    "dons", "edelsteen", "eendenveren", "elastaan", 
    "glas", "halfedelsteen", "hennep", "ijzer", "imitatiebont", "imitatiesuede", "jute", 
    "kapok", "kasjmier", "katoen", "kokosvezel", "kristal", "kurk", "leer", "linnen", 
    "lyocell", "messing", "metaal", "metaaldraad", "microleer", "imitatieleer",
    "mohair", "neopreen", "palmbladeren", "papier", "parel", "polyacryl",
    "polyamide", "polyester", "polyethyleen", "polylactide", 
    "polyolefins", "polypropyleen", "polyurethaan", "ramie", "riet", "rubber", "rvs", 
    "siliconen", "sterling zilver", "stro", "suede", "viscose", "wol", 
    "zijde", "zink", "koper", "polycarbonaat", "titanium"
]

# Dictionary for edge case mappings
edge_case_mappings = {
    "leer": "leer", 
    "polycarbonaat": "polycarbonaat", 
    "nylon": "polyamide", 
    "pu": "polyurethaan", 
    "staal": "staal",
    "metaal": "metaal",
    "cotton": "katoen", 
    "rayon": "viscose", 
    "suede": "leer",
    "koper": "koper",
    "tencel": "lyocell",
    "spandex": "elastaan",
    "elastan": "elastaan",
    "elasthaan": "elastaan",
    "tpu": "polyurethaan",
    "modal": "viscose",
    "bamboo": "bamboe",
    "elasthan": "elastaan",
    "elastane": "elastaan",
    "nubuck": "leer",
    "lycra": "elastaan",
    "vinyl": "pvc",
    "cashmere": "kasjmier",
    "merinowol": "wol",
    "schapensleer": "leer",
    "schaapsleer": "leer",
    "buffelleer": "leer",
    "koeienleer": "leer",
    "coated leer": "leer",
    "schapenleer": "leer",
    "rundleer": "leer",
    "runderleer": "leer"
}

# # Define a dictionary for manual overrides
manual_overrides = {
"16231574": ["katoen - 80", "polyester - 20"],
"16231596": ["katoen - 80", "polyester - 20"],
"16643268": ["katoen - 80, polyester - 20"],
"16646435": ["polyamide - 60, polyester - 25, elastaan 15"],
"17200566": ["polyester 88, elastaan - 12"],
"17200577": ["polyester 88, elastaan - 12"],
"17200582": ["polyester 87, elastaan - 13"],
"17200560": ["polyester 90, elastaan - 10"]
}

# Function to normalize text
def normalize_text(text):
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')

def process_material_udf(material_value, article_id):
    if not material_value:
        return {"materials": [], "edge_case_flag": "No"}
    
    results = []
    edge_case_used = False
    material_parts = re.split(r"[,/]", material_value)  # Split by commas and slashes
    
    for part in material_parts:
        # Extract percentage and material name
        percentage_match = re.search(r"(\d+)%", part)  # Match percentage
        percentage = percentage_match.group(1) if percentage_match else None
        
        # Normalize and clean the material name
        raw_material = normalize_text(re.sub(r"[\d%]+|\s+", " ", part).strip().lower())
        
        # Check for manual overrides first
        if article_id in manual_overrides:
            # Use manual override without modification, including percentage
            results.extend(manual_overrides[article_id])  # Add manual override materials
            edge_case_used = True  # Set edge case flag to True
        else:
            # Check if the material is mapped using an edge case
            if raw_material in edge_case_mappings:
                edge_case_used = True
                mapped_material = edge_case_mappings[raw_material]
            else:
                mapped_material = raw_material
            
            # Validate material against the predefined list
            if mapped_material in materials_list:
                # Only set percentage to 100 if it's not in the manual overrides
                if percentage is None:  # Add percentage only if it is missing
                    percentage_value = "100"
                else:
                    percentage_value = percentage
                
                results.append(f"{mapped_material} - {percentage_value}")
            # else:
                # Material is not in the list; exclude it
                # display(f"Excluded unmatched material: {raw_material}")
    
    return {"materials": results, "edge_case_flag": "Yes" if edge_case_used else "No"}



# Register the UDF with a structured return type
schema = StructType([ 
    StructField("materials", ArrayType(StringType()), True), 
    StructField("edge_case_flag", StringType(), True) 
])

process_material = F.udf(process_material_udf, schema)

# Apply the UDF to process materials and percentages, passing article_id
inbound_with_matches = inbound_material_weight.withColumn(
    "Processed_Data",
    process_material(F.col("Y02 - materiaal"), F.col("article_id"))
)

# Extract materials and edge case flag into separate columns
inbound_with_matches = inbound_with_matches.withColumn(
    "Materials_Percentages",
    F.col("Processed_Data.materials")
).withColumn(
    "Edge_Case_Flag",
    F.col("Processed_Data.edge_case_flag")
).drop("Processed_Data")

edge_case_count = inbound_with_matches.filter(F.col("Edge_Case_Flag") == "Yes").count()

# Handle rows with single materials by setting their percentage to 100 if not already present
inbound_with_matches = inbound_with_matches.withColumn(
    "Materials_Percentages_Fixed",
    F.when(
        (F.size(F.col("Materials_Percentages")) == 1) & 
        (~F.col("Materials_Percentages")[0].rlike(r" - \d+$")),  # Check if percentage already exists
        F.array(F.concat(F.col("Materials_Percentages")[0], F.lit(" - 100")))  # Append only if missing
    ).otherwise(F.col("Materials_Percentages"))
)

# Separate matched and unmatched articles
non_matched_articles = inbound_with_matches.filter(
    F.size(F.col("Materials_Percentages_Fixed")) == 0
)

matched_articles = inbound_with_matches.filter(
    F.size(F.col("Materials_Percentages_Fixed")) > 0
)

# Display results
display(matched_articles.select("article_id", "mcc_1_name", "Y02 - materiaal", "Materials_Percentages_Fixed", "Edge_Case_Flag", "bss_2_name"))
display(non_matched_articles.select("article_id", "Y02 - materiaal", "mcc_1_name", "mcc_2_name", "preferred_supplier_name", "net_despatch_qty", "business_model_desc", "article_gross_weight_num"))

# Save the results as temporary views
matched_articles.createOrReplaceTempView("matched_articles_view")
non_matched_articles.createOrReplaceTempView("non_matched_articles_view")

# Create a DataFrame for the edge case count
edge_case_count_df = spark.createDataFrame(
    [(edge_case_count,)], 
    ["Edge_Case_Flag_Count"]
)

# Display the edge case count as a DataFrame
display(edge_case_count_df)

# COMMAND ----------

# Databricks notebook source
# Material Share Analysis per BSS_2_name
# Shows BSS_2 categories containing polyester, katoen, or viscose with full material distribution
# Filtered for Private Label business model only

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

# COMMAND ----------

# DBTITLE 1,Load and process data
# Load the matched articles view created in the previous notebook
matched_articles = spark.table("matched_articles_view")

# Filter for Private Label business model only
matched_articles = matched_articles.filter(F.col("business_model_desc") == "Private label")

print(f"Total articles in Private Label: {matched_articles.count()}")

# Explode the Materials_Percentages_Fixed array to get one row per material-percentage pair
exploded_materials = matched_articles.select(
    "article_id",
    "bss_2_name",
    "mcc_1_name",
    "net_despatch_qty",
    "article_gross_weight_num",
    F.explode("Materials_Percentages_Fixed").alias("Material_Percentage_Pair")
)

# Extract material name and percentage
exploded_materials = exploded_materials.withColumn(
    "Material",
    F.trim(F.regexp_extract(F.col("Material_Percentage_Pair"), r"^(.+?) - \d+", 1))
).withColumn(
    "Percentage",
    F.regexp_extract(F.col("Material_Percentage_Pair"), r"- (\d+\.?\d*)$", 1).cast(FloatType())
).filter(F.col("Percentage").isNotNull())

# Calculate material weight: (Percentage / 100) * (gross_weight_kg) * quantity
exploded_materials = exploded_materials.withColumn(
    "Material_Weight_kg",
    (F.col("Percentage") / 100) * (F.col("article_gross_weight_num") / 1000) * F.col("net_despatch_qty")
)

# COMMAND ----------

# DBTITLE 1,Calculate material share per BSS_2_name (filtered for polyester, katoen, viscose)
# Group by MCC_1_name, BSS_2_name and Material to get total weight per material per category
material_weight_per_bss2 = exploded_materials.groupBy("mcc_1_name", "bss_2_name", "Material").agg(
    F.sum("Material_Weight_kg").alias("Total_Material_Weight_kg")
)

# Find BSS_2_name categories that contain polyester, katoen, or viscose
target_materials = ["polyester", "katoen", "viscose"]
bss2_with_target_materials = material_weight_per_bss2.filter(
    F.col("Material").isin(target_materials)
).select("mcc_1_name", "bss_2_name").distinct()

# Filter to only include these BSS_2_name categories (but show ALL materials in them)
material_weight_per_bss2_filtered = material_weight_per_bss2.join(
    bss2_with_target_materials,
    on=["mcc_1_name", "bss_2_name"],
    how="inner"
)

# Calculate total weight per MCC_1_name and BSS_2_name
total_weight_per_bss2 = material_weight_per_bss2_filtered.groupBy("mcc_1_name", "bss_2_name").agg(
    F.sum("Total_Material_Weight_kg").alias("Total_BSS2_Weight_kg")
)

# Join and calculate percentage share
material_share_final = material_weight_per_bss2_filtered.join(
    total_weight_per_bss2,
    on=["mcc_1_name", "bss_2_name"],
    how="left"
).withColumn(
    "Material_Share_Percentage",
    F.round(F.expr("try_divide(Total_Material_Weight_kg, Total_BSS2_Weight_kg)") * 100, 2)
).filter(
    (F.col("Total_BSS2_Weight_kg") > 0) & (F.col("Material_Share_Percentage").isNotNull())
).select(
    "mcc_1_name",
    "bss_2_name",
    "Material",
    "Material_Share_Percentage",
    "Total_Material_Weight_kg",
    "Total_BSS2_Weight_kg"
).orderBy(
    F.col("mcc_1_name"),
    F.col("bss_2_name"),
    F.col("Material_Share_Percentage").desc()
)

# COMMAND ----------

# DBTITLE 1,Final Output - Private Label BSS_2 categories with polyester/katoen/viscose and their full material distribution
display(material_share_final)

# COMMAND ----------

from pyspark.sql import functions as F

# Select relevant columns from the non_matched_articles DataFrame
non_matched_articles = non_matched_articles.select(
    "article_id", "article_desc", "preferred_supplier_name", "article_gross_weight_num", "mcc_3_name", "net_despatch_qty", "business_model_desc", "country_code", "article_gross_weight_num"
)

# Calculate total weight using net_despatch_qty and article_gross_weight_num
non_matched_articles = non_matched_articles.withColumn(
    "Total_Weight",
    (F.col("net_despatch_qty") * F.col("article_gross_weight_num") / 1000)
)

# Group by relevant columns and sum the total weight
non_material_weight_summary_df = non_matched_articles.groupBy(
    "mcc_3_name", "preferred_supplier_name", "article_desc", "article_id", "business_model_desc", "country_code", "net_despatch_qty"
).agg(
    F.round(F.sum("Total_Weight"), 2).alias("Total_Weight")
)

# List of specific mcc_3_name values
mcc_3_names = [
    "PW7C - Mens Casual Trousers", "MX5M - Ladies Shorts", "RX5X - Ladies Jeans",
    "WX6H - Ladies Briefs", "FJ3G - Ladies Cardigans", "JC9C - Ladies T-shirts",
    "VW7V - Ladies Jackets", "QM2R - Ladies Non-wired Bras", "CW4M - Ladies Denim Shorts",
    "CR9V - Ladies Beach Bottoms Other", "WG8G - Ladies Knitwear Other", "PX8M - Ladies Pullovers",
    "RQ7H - Mens Chino", "RJ6J - Ladies Trousers", "WR2V - Baby Changing & Bath",
    "MP4C - Ladies Capris", "RX9Q - Girls Pullovers & Sweats", "PX4F - Ladies Tops Knitted",
    "XR5Q - Girls Longsleeves", "FH8X - Ladies Dresses (Alliance)", "VM8G - Mens Knitwear",
    "VX9M - Ladies Singlets", "VP9Q - Girls Shortsleeves", "RM8Q - Ladies Sports Shirts",
    "CR2C - Girls Skirts", "VC7R - Mens Sweats", "CR5F - Cushion & Covers", "XJ9X - Mens Formal Shirts",
    "FH4W - Ladies Pyjama Tops", "QJ6F - Mens Polos", "WF2V - Mens Sports Sweaters", "QH3G - Ladies Outerwear Other",
    "QW3G - Ladies Sports Sweaters", "JF2G - Ladies Sweaters", "PJ6M - Mens Casual Shirts", "GJ2M - Ladies Blazers",
    "HP6V - Ladies Tops Woven", "XF6P - Ladies Sports Pants", "QR5C - Ladies Fashion Scarves", "MJ4J - Girls Shorts",
    "CH3F - Mens Jackets", "VM2R - Ladies Nightwear Other", "MF5G - Boys Shortsleeves", "XJ2G - Mens Jeans",
    "GQ8F - Ladies Skirts", "RF4H - Ladies Dresses Knitted", "PR4R - Ladies Pushup Bikini Tops", "CP7W - Ladies Socks",
    "CJ2M - Girls Jeans", "HR6Q - Ladies Bodies", "JP8V - Ladies Sports Jackets", "JV2G - Boys Shirts",
    "QP9R - Ladies Winter Scarves", "GJ7C - Mens Ski Socks", "VW5W - Ladies Shapewear Shorts", "RJ2X - Ladies Underwire Bikini Tops",
    "WJ8F - Girls Bikinis Other", "WG6P - Ladies Underwear Vests", "VR8F - Ladies Sports Shorts", "WF7W - Mens Sports Shirts",
    "HQ9H - Ladies Blouses", "MR9F - Ladies Sportswear Other", "CR9W - Ladies Beach Briefs", "WG4H - Boys Pullovers & Sweats",
    "HP9V - Ladies Jegging", "HG2R - Girls Beachwear Other", "GP2Q - Ladies Hipsters", "FC5R - Maternity Other",
    "CQ8F - Ladies Jumpsuits", "PJ3F - Girls Trousers", "QV3M - Mens Formal Jackets", "RX2Q - Boys Sleepwear", "JH2J - Baby Bedding",
    "JQ4R - Ladies Sports Bras", "GF6R - Mens Parka Jackets", "XV5X - Boys Outerwear", "CJ3X - Bath Textiles",
    "GP7G - Ladies Push-up Bras", "VQ9M - Mens Jogging Bottoms", "QV9X - Boys Trousers", "XR8H - Girls Sport Swimsuits",
    "XF2X - Ladies Treggings", "QC6C - Boys Shorts", "PM6X - Mens Loungewear Bottoms", "MG8M - Girls Jumpsuits (incl. box suit)",
    "XP7F - Ladies Shapewear Vests", "HF2C - Ladies Sport Swimsuits", "MP7R - Ladies Padded Outerwear", "HF5G - Duvet Cover Sets",
    "QR5F - Ladies Shaping Swimsuits Cupsizes", "PF6M - Mens Swim Shorts", "QF5J - Mens Sports Jackets", "JX5C - Mens Bodywarmers",
    "MH7R - Ladies Nightshirts", "VQ4H - Ladies Bermuda Shorts", "QH5V - Mens Underwear Other", "HC5X - Ladies Pyjama Sets",
    "CJ6J - Mens Pyjama Sets", "RQ6Q - Ladies Bikini Sets", "PJ4H - Ladies Dresses Woven", "CJ5J - Ladies Shortamas",
    "FX7W - Beachwear Clothing", "WR6C - Ladies Denim Jackets", "VJ2G - Boys Sports Pants", "JQ4C - Ladies Shaping Swimsuits",
    "WF9G - Mens Winter Scarves", "FQ6W - Ladies Raincoats", "FM5M - Ladies Tunics Woven", "PM8G - Mens Chino Shorts",
    "QX2V - Pillowcases", "GF7X - Boys Swim Boxers", "QX4M - Ladies Ski Socks", "MQ3P - Mens Sports Shorts", "JC9H - Ladies Leggings",
    "QC6V - Ladies Parkas", "MQ3M - Ladies Brazilians", "HR7P - Girls Sports Jackets", "WQ2V - Mens Padded Coats",
    "QG3P - Boys Sports Tracksuits", "MG4X - Mens Boxershorts", "WJ3X - Ladies Underwear Shorts", "FQ2X - Ladies Thongs",
    "CM8Q - Girls Tights", "FG3M - Mens Knit Cardigans", "CR9M - Ladies Bodies Shapewear", "WR4V - Mens Shorts Other",
    "PC3H - Boys Underwear (incl. romper)", "MW2V - Girls Sleepwear", "PC7X - Boys Beachwear Other", "WR8H - Mens Underwear T-shirts",
    "WX9G - Ladies Bathrobes", "CQ4Q - Ladies Playsuits", "XW5R - Girls Sports Shirts", "PJ4P - Ties & Bow Ties",
    "QR8Q - Ladies Triangle Bikini Tops", "GJ5Q - Childrens Ski Socks", "WV8R - Ladies Crop Bikini Tops", "GP4H - Fitted Sheets",
    "HX9G - Ladies Shapewear Briefs", "PV3C - Girls Triangle Bikinis", "JF7J - Mens Sweat Shorts", "XP4R - Boys Sports Sweaters",
    "XH2X - Ladies Pyjama Bottoms", "CQ4G - Ladies Swimsuits", "VW2F - Boys Sports Shirts", "FR2H - Mens Thermo", "HC2X - Beach Towels",
    "XF6Q - Mens Loungewear Tops", "HM9W - Ladies Maxibriefs", "WX8P - Ladies Tunics (Alliance)", "RV2M - Mens Cargo Shorts",
    "CJ7P - Mens Fashion Scarves", "RQ9M - Boys Sports Jackets", "VQ5C - Ladies Maxi Dresses", "JX3V - Maternity Beachwear",
    "QP7Q - Ladies Beach Tie-knot Briefs", "RC2G - Mens Pyjama Bottoms", "MR5H - Ladies Slipdresses", "VR9C - Ladies Beach Folded Briefs",
    "GJ7Q - Mens Cargo Trousers", "QP6C - Mens Coats", "WX7F - Sports Socks", "QG3J - Mens Bathrobes", "PC6F - Ladies Loungewear Bottoms",
    "CR2R - Boys Onesies (incl. box suit)", "CG9P - Mens Denim Jackets", "GC4C - Girls Sports Pants", "FW7H - Sarongs",
    "JF3J - Ladies Leather & PU Outerwear", "HF8C - Ladies Loose Collars", "GR5X - Ladies Tunics Knitted", "GW7V - Ladies House Suits",
    "JM9C - Ladies Halter Bikini Tops", "QW5F - Ladies Swimsuits Cupsizes", "QG9J - Mens Briefs", "WR9M - Girls Sports Shorts",
    "MG6C - Girls Sports Sweaters", "XR3P - Mens Nightwear Other", "VJ6M - Maternity Tops", "PR4C - Legwear Other", "XH6R - Mens Underwear Vests",
    "GR7H - Boys Sets", "RX4Q - Baby Boxkleden", "FR6F - Mens Beachwear Other", "HX2W - Ladies Shapewear Bottoms Other",
    "GR6C - Girls Swimsuits", "MP9X - Ladies Minimizers", "JF7F - Girls Underwear (incl. romper)", "PV9M - Ladies Boleros",
    "MF4G - Mens Rain Coats", "FC2C - Mens Pyjama Tops", "GX6W - Ladies Tankini Tops", "XQ2H - Mens Underwear Sports",
    "QH5X - Mattress Protectors", "FG7P - Mens Leather Jackets", "CX7X - Ladies Maternity Bras", "GQ4R - Ladies Lingerie Other",
    "MR7H - Ladies Shapewear Dresses", "MP8W - Girls Sportswear Other", "HV9C - Ladies Sports Tracksuits", "VP4Q - Girls Sports Tracksuits",
    "CJ9P - Ladies Tops (Alliance)", "QF6P - Girls Outerwear", "VQ4Q - Mens Socks", "GC3Q - Ladies Coats", "QV4P - Childrens Socks",
    "XW9C - Ladies Bikini Tops Other", "PM9Q - Mens T-shirts", "FM4M - Ladies Wired Bras", "GP8C - Ladies Loungewear Tops",
    "XM7V - Girls Dresses", "RH2R - Mens Denim Shorts", "FX5J - Mens Swim Boxers", "WH9C - Ladies Padded Bras",
    "XF6C - Mens Sweats Cardigan", "JM7P - Boys Jeans", "XV9J - Ladies Bottoms Other", "FX5W - Maternity Bottoms", "VM3J - Ladies Gloves",
    "XH7Q - Girls Leggings", "JH5H - Ladies Bras Other", "JQ9F - Ladies Tights", "RX9H - Mens Sports Tracksuits", "XF2F - Mens Sports Pants",
    "CX2P - Boys Longsleeves", "PG9M - Boys Swim Shorts", "HG6M - Girls Sets", "VQ7P - Ladies Underwear Accessories", "VC9M - Ladies Bandeau Bikini Tops",
    "RF7M - Boys Sports Shorts", "VG7V - Mens Gloves", "VX9V - Kitchen & Table Textiles"
]

suppliers = [
    "Sisters Point A/S", "Sassa Mode GmbH", "Underwear Sweden AB", 
    "ABA Fashion Limited", "Mos Mosh A/S", "Zizzi Denmark ApS", 
    "Lee Tai Sang Swimwear Factory Ltd", "Mac Mode GmbH & Co. KGaA", "Dutch Home Company B.V/Arli Group B.V.", 
    "B-Boo Baby & Lifestyle GmbH", "DK Company Vejle A/S", "TGS DIS TICARET A.S.",
    "Tas Textiles India Private Ltd.", "DKH Retail Limited", "Röhnisch Sportswear AB",
    "Madam Stoltz Aps", "Threebyone Europe AB", "Sofie Schnoor A/S", "Lyle & Scott Ltd", 
    "Nümph A/S", "Levi Strauss & Co Europe", "Modström", "JL Company Ltd", "Aim Apparel AB", "Guess Europe sagl", 
    "Soya Concept as", "Töjfabrik Aps / Wear Group A/S", "Bloomingville A/S", "Tenson AB", "United Swimwear Apparel Co.,Ltd", 
    "Falke KGAA", "Jack Wolfskin Retail GmbH", "Moss CPH A/S", "Van De Velde NV", "Ralph Lauren France Sas", 
    "Killtec Sport- und Freizeit GmbH", "EOZ NV", "Tonsure ApS", "Champion Products Europe Ltd.", "Magic Apparels Ltd./Red Button", 
    "Mamsy", "ILERI GIYIM SAN VE DIS TIC.STI.", "Zhejiang Sungin Industry Group co.,Ltd.", "Playshoes GmbH", "Rosemunde Aps", 
    "Society of Lifestyle APS", "Noman Terry Towel Mills Limited", "Ningbo China-Blue Fashion Co., Ltd", "All Sport N.V.", 
    "Brands4kids A/S", "Sweatertech Ltd.", "Brands of Scandinavia A/S", "CWF Children Worldwide Fashion", "Nore Tekstil Sanayi Ve Ticaret Ltd.Sti.", 
    "KM Apparel Knit (Pvt) Ltd.", "Udare Ltd.", "CKS Fashion", "King Wings International Trading Co., Ltd", "A.R.W. NV", 
    "Ningbo Leader Garment Co. LTD", "VERVALLEN Focus International Ltd", "W.S. Engross APS", "Sona Enterprises", "Renfold Ltd", 
    "CECEBA Group GmbH", "Sandgaard A/S", "TB International GmbH", "Kam International", "Haddad Brands Europe", "Urban Brands ApS", 
    "Bruuns Bazaar A/S", "WDKX Ltd ($)", "Mads Norgaard - Copenhagen A/S", "Tamara Tekstil Sanayi ve Dış Ticaret Ltd Sti", 
    "GANT DACH GmbH", "Regatta ltd.", "Mat Fashion", "Sportswear Company s.p.a.(Stone Island)", "New Visions Berlin GmbH", 
    "Stone Island Distribution S.R.L.", "VAUDE SPORT GmbH & Co. KG", "Lexson Brands B.V.", "J Carter Sporting Club Ltd - Castore", 
    "Nudie Jeans Marketing AB", "Marc O'Polo International GmbH", "M.G. Ekkelboom B.V.", "Liu Jo Benelux N.V.", "Treasure Will Limited", 
    "And Bording Aps","Choice Clothing co. PVT.LTD", "Sports Group Denmark A/S", "NÜ A/S / Zoey", "Westex India", 
    "Shiv Shakti Home", "AycemTekstil San.Tic. Ltd.ŞTİ.", "Olymp Bezner Kg", "Rugs Creation", "ISYGS", "Pacific Jeans Ltd.", 
    "Medico sports fashion GmbH", "PNG Textiles Pvt Ltd", "Brave kid SRL", "Grimmer & Sommacal", "Felina gmbh", 
    "Hangzhou Bestsino Imp & Exp Co Ltd", "F&H of Scandinavia A/S", "Sharda Exports", "Laaj International", "Jai Knits Creations", 
    "Woolrich Europe S.p.A", "Bestwin (Shanghai) Home Fashion Ltd", "Didriksons Deutschland GmbH", "The Trade Aid Company", 
    "Peninsula Fashion Knitwear Limited", "Sovedam", "Ever-Glory Int. Group Apparel inc.", "Zaber & Zubair Fabrics Ltd", 
    "WDKX Ltd. (€)", "Yab Yum Clothing Co. Aps", "HTS Textilvertriebs GmbH", "DIESEL SPA", "Industrias Plasticas IGOR", 
    "Fashion Club 70 N.V.", "Levi's Footwear & Accessories (Switzerland) SA", "ParkStor Tekstil Tic.Ltd.Sti.", "Sumec Textile & Light Industry Co Ltd", 
    "Liewood A/S", "Coram DIY NV", "L.C. Jordan Company Limited", "Norban Comtex Ltd.", "Brand Machine International Limited"
]

filtered_non_material_weight_summary_df = non_material_weight_summary_df.filter(
    F.col("mcc_3_name").isin(mcc_3_names) & F.col("preferred_supplier_name").isin(suppliers)
)

# Display the result
display(filtered_non_material_weight_summary_df)
total_weight = filtered_non_material_weight_summary_df.groupBy().sum("Total_Weight").collect()[0][0]
total_net_despatch = filtered_non_material_weight_summary_df.groupBy().sum("net_despatch_qty").collect()[0][0]
display(total_net_despatch)
display(total_weight)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

# Split "Materials_Percentages" into separate material-percentage pairs
exploded_articles = matched_articles.withColumn(
    "Material_Percentage_Pair", 
    F.explode(F.split(F.concat_ws(", ", F.col("Materials_Percentages")), ", "))
)

# Extract Material
exploded_articles = exploded_articles.withColumn(
    "Material", F.regexp_extract(F.col("Material_Percentage_Pair"), r"^(.*?) -", 1)
)

# Extract Percentage with validation (handle empty and malformed values)
exploded_articles = exploded_articles.withColumn(
    "Percentage",
    F.when(F.trim(F.regexp_extract(F.col("Material_Percentage_Pair"), r"- (\d+\.?\d*)$", 1)) != "",  
           F.regexp_extract(F.col("Material_Percentage_Pair"), r"- (\d+\.?\d*)$", 1).cast(FloatType())
    ).otherwise(None)  # Assign NULL for invalid values
)

# Filter out rows where Percentage is NULL (invalid/malformed input)
exploded_articles = exploded_articles.filter(F.col("Percentage").isNotNull())

# Calculate Material Weight
exploded_articles = exploded_articles.withColumn(
    "Material_Weight", 
    (F.col("Percentage") / 100) * (F.col("article_gross_weight_num") / 1000) * F.col("net_despatch_qty")
)

# Aggregate material weights
material_weight_summary_df = exploded_articles.groupBy(
    "Material", "mcc_3_name", "preferred_supplier_name", "article_desc", 
    "article_id", "business_model_desc", "country_code", "net_despatch_qty"
).agg(
    F.round(F.sum("Material_Weight"), 2).alias("Total_Weight")
)

# List of specific mcc_3_name values
mcc_3_names = [
    "PW7C - Mens Casual Trousers", "MX5M - Ladies Shorts", "RX5X - Ladies Jeans",
    "WX6H - Ladies Briefs", "FJ3G - Ladies Cardigans", "JC9C - Ladies T-shirts",
    "VW7V - Ladies Jackets", "QM2R - Ladies Non-wired Bras", "CW4M - Ladies Denim Shorts",
    "CR9V - Ladies Beach Bottoms Other", "WG8G - Ladies Knitwear Other", "PX8M - Ladies Pullovers",
    "RQ7H - Mens Chino", "RJ6J - Ladies Trousers", "WR2V - Baby Changing & Bath",
    "MP4C - Ladies Capris", "RX9Q - Girls Pullovers & Sweats", "PX4F - Ladies Tops Knitted",
    "XR5Q - Girls Longsleeves", "FH8X - Ladies Dresses (Alliance)", "VM8G - Mens Knitwear",
    "VX9M - Ladies Singlets", "VP9Q - Girls Shortsleeves", "RM8Q - Ladies Sports Shirts",
    "CR2C - Girls Skirts", "VC7R - Mens Sweats", "CR5F - Cushion & Covers", "XJ9X - Mens Formal Shirts",
    "FH4W - Ladies Pyjama Tops", "QJ6F - Mens Polos", "WF2V - Mens Sports Sweaters", "QH3G - Ladies Outerwear Other",
    "QW3G - Ladies Sports Sweaters", "JF2G - Ladies Sweaters", "PJ6M - Mens Casual Shirts", "GJ2M - Ladies Blazers",
    "HP6V - Ladies Tops Woven", "XF6P - Ladies Sports Pants", "QR5C - Ladies Fashion Scarves", "MJ4J - Girls Shorts",
    "CH3F - Mens Jackets", "VM2R - Ladies Nightwear Other", "MF5G - Boys Shortsleeves", "XJ2G - Mens Jeans",
    "GQ8F - Ladies Skirts", "RF4H - Ladies Dresses Knitted", "PR4R - Ladies Pushup Bikini Tops", "CP7W - Ladies Socks",
    "CJ2M - Girls Jeans", "HR6Q - Ladies Bodies", "JP8V - Ladies Sports Jackets", "JV2G - Boys Shirts",
    "QP9R - Ladies Winter Scarves", "GJ7C - Mens Ski Socks", "VW5W - Ladies Shapewear Shorts", "RJ2X - Ladies Underwire Bikini Tops",
    "WJ8F - Girls Bikinis Other", "WG6P - Ladies Underwear Vests", "VR8F - Ladies Sports Shorts", "WF7W - Mens Sports Shirts",
    "HQ9H - Ladies Blouses", "MR9F - Ladies Sportswear Other", "CR9W - Ladies Beach Briefs", "WG4H - Boys Pullovers & Sweats",
    "HP9V - Ladies Jegging", "HG2R - Girls Beachwear Other", "GP2Q - Ladies Hipsters", "FC5R - Maternity Other",
    "CQ8F - Ladies Jumpsuits", "PJ3F - Girls Trousers", "QV3M - Mens Formal Jackets", "RX2Q - Boys Sleepwear", "JH2J - Baby Bedding",
    "JQ4R - Ladies Sports Bras", "GF6R - Mens Parka Jackets", "XV5X - Boys Outerwear", "CJ3X - Bath Textiles",
    "GP7G - Ladies Push-up Bras", "VQ9M - Mens Jogging Bottoms", "QV9X - Boys Trousers", "XR8H - Girls Sport Swimsuits",
    "XF2X - Ladies Treggings", "QC6C - Boys Shorts", "PM6X - Mens Loungewear Bottoms", "MG8M - Girls Jumpsuits (incl. box suit)",
    "XP7F - Ladies Shapewear Vests", "HF2C - Ladies Sport Swimsuits", "MP7R - Ladies Padded Outerwear", "HF5G - Duvet Cover Sets",
    "QR5F - Ladies Shaping Swimsuits Cupsizes", "PF6M - Mens Swim Shorts", "QF5J - Mens Sports Jackets", "JX5C - Mens Bodywarmers",
    "MH7R - Ladies Nightshirts", "VQ4H - Ladies Bermuda Shorts", "QH5V - Mens Underwear Other", "HC5X - Ladies Pyjama Sets",
    "CJ6J - Mens Pyjama Sets", "RQ6Q - Ladies Bikini Sets", "PJ4H - Ladies Dresses Woven", "CJ5J - Ladies Shortamas",
    "FX7W - Beachwear Clothing", "WR6C - Ladies Denim Jackets", "VJ2G - Boys Sports Pants", "JQ4C - Ladies Shaping Swimsuits",
    "WF9G - Mens Winter Scarves", "FQ6W - Ladies Raincoats", "FM5M - Ladies Tunics Woven", "PM8G - Mens Chino Shorts",
    "QX2V - Pillowcases", "GF7X - Boys Swim Boxers", "QX4M - Ladies Ski Socks", "MQ3P - Mens Sports Shorts", "JC9H - Ladies Leggings",
    "QC6V - Ladies Parkas", "MQ3M - Ladies Brazilians", "HR7P - Girls Sports Jackets", "WQ2V - Mens Padded Coats",
    "QG3P - Boys Sports Tracksuits", "MG4X - Mens Boxershorts", "WJ3X - Ladies Underwear Shorts", "FQ2X - Ladies Thongs",
    "CM8Q - Girls Tights", "FG3M - Mens Knit Cardigans", "CR9M - Ladies Bodies Shapewear", "WR4V - Mens Shorts Other",
    "PC3H - Boys Underwear (incl. romper)", "MW2V - Girls Sleepwear", "PC7X - Boys Beachwear Other", "WR8H - Mens Underwear T-shirts",
    "WX9G - Ladies Bathrobes", "CQ4Q - Ladies Playsuits", "XW5R - Girls Sports Shirts", "PJ4P - Ties & Bow Ties",
    "QR8Q - Ladies Triangle Bikini Tops", "GJ5Q - Childrens Ski Socks", "WV8R - Ladies Crop Bikini Tops", "GP4H - Fitted Sheets",
    "HX9G - Ladies Shapewear Briefs", "PV3C - Girls Triangle Bikinis", "JF7J - Mens Sweat Shorts", "XP4R - Boys Sports Sweaters",
    "XH2X - Ladies Pyjama Bottoms", "CQ4G - Ladies Swimsuits", "VW2F - Boys Sports Shirts", "FR2H - Mens Thermo", "HC2X - Beach Towels",
    "XF6Q - Mens Loungewear Tops", "HM9W - Ladies Maxibriefs", "WX8P - Ladies Tunics (Alliance)", "RV2M - Mens Cargo Shorts",
    "CJ7P - Mens Fashion Scarves", "RQ9M - Boys Sports Jackets", "VQ5C - Ladies Maxi Dresses", "JX3V - Maternity Beachwear",
    "QP7Q - Ladies Beach Tie-knot Briefs", "RC2G - Mens Pyjama Bottoms", "MR5H - Ladies Slipdresses", "VR9C - Ladies Beach Folded Briefs",
    "GJ7Q - Mens Cargo Trousers", "QP6C - Mens Coats", "WX7F - Sports Socks", "QG3J - Mens Bathrobes", "PC6F - Ladies Loungewear Bottoms",
    "CR2R - Boys Onesies (incl. box suit)", "CG9P - Mens Denim Jackets", "GC4C - Girls Sports Pants", "FW7H - Sarongs",
    "JF3J - Ladies Leather & PU Outerwear", "HF8C - Ladies Loose Collars", "GR5X - Ladies Tunics Knitted", "GW7V - Ladies House Suits",
    "JM9C - Ladies Halter Bikini Tops", "QW5F - Ladies Swimsuits Cupsizes", "QG9J - Mens Briefs", "WR9M - Girls Sports Shorts",
    "MG6C - Girls Sports Sweaters", "XR3P - Mens Nightwear Other", "VJ6M - Maternity Tops", "PR4C - Legwear Other", "XH6R - Mens Underwear Vests",
    "GR7H - Boys Sets", "RX4Q - Baby Boxkleden", "FR6F - Mens Beachwear Other", "HX2W - Ladies Shapewear Bottoms Other",
    "GR6C - Girls Swimsuits", "MP9X - Ladies Minimizers", "JF7F - Girls Underwear (incl. romper)", "PV9M - Ladies Boleros",
    "MF4G - Mens Rain Coats", "FC2C - Mens Pyjama Tops", "GX6W - Ladies Tankini Tops", "XQ2H - Mens Underwear Sports",
    "QH5X - Mattress Protectors", "FG7P - Mens Leather Jackets", "CX7X - Ladies Maternity Bras", "GQ4R - Ladies Lingerie Other",
    "MR7H - Ladies Shapewear Dresses", "MP8W - Girls Sportswear Other", "HV9C - Ladies Sports Tracksuits", "VP4Q - Girls Sports Tracksuits",
    "CJ9P - Ladies Tops (Alliance)", "QF6P - Girls Outerwear", "VQ4Q - Mens Socks", "GC3Q - Ladies Coats", "QV4P - Childrens Socks",
    "XW9C - Ladies Bikini Tops Other", "PM9Q - Mens T-shirts", "FM4M - Ladies Wired Bras", "GP8C - Ladies Loungewear Tops",
    "XM7V - Girls Dresses", "RH2R - Mens Denim Shorts", "FX5J - Mens Swim Boxers", "WH9C - Ladies Padded Bras",
    "XF6C - Mens Sweats Cardigan", "JM7P - Boys Jeans", "XV9J - Ladies Bottoms Other", "FX5W - Maternity Bottoms", "VM3J - Ladies Gloves",
    "XH7Q - Girls Leggings", "JH5H - Ladies Bras Other", "JQ9F - Ladies Tights", "RX9H - Mens Sports Tracksuits", "XF2F - Mens Sports Pants",
    "CX2P - Boys Longsleeves", "PG9M - Boys Swim Shorts", "HG6M - Girls Sets", "VQ7P - Ladies Underwear Accessories", "VC9M - Ladies Bandeau Bikini Tops",
    "RF7M - Boys Sports Shorts", "VG7V - Mens Gloves", "VX9V - Kitchen & Table Textiles"
]

suppliers = [
    "Sisters Point A/S", "Sassa Mode GmbH", "Underwear Sweden AB", 
    "ABA Fashion Limited", "Mos Mosh A/S", "Zizzi Denmark ApS", 
    "Lee Tai Sang Swimwear Factory Ltd", "Mac Mode GmbH & Co. KGaA", "Dutch Home Company B.V/Arli Group B.V.", 
    "B-Boo Baby & Lifestyle GmbH", "DK Company Vejle A/S", "TGS DIS TICARET A.S.",
    "Tas Textiles India Private Ltd.", "DKH Retail Limited", "Röhnisch Sportswear AB",
    "Madam Stoltz Aps", "Threebyone Europe AB", "Sofie Schnoor A/S", "Lyle & Scott Ltd", 
    "Nümph A/S", "Levi Strauss & Co Europe", "Modström", "JL Company Ltd", "Aim Apparel AB", "Guess Europe sagl", 
    "Soya Concept as", "Töjfabrik Aps / Wear Group A/S", "Bloomingville A/S", "Tenson AB", "United Swimwear Apparel Co.,Ltd", 
    "Falke KGAA", "Jack Wolfskin Retail GmbH", "Moss CPH A/S", "Van De Velde NV", "Ralph Lauren France Sas", 
    "Killtec Sport- und Freizeit GmbH", "EOZ NV", "Tonsure ApS", "Champion Products Europe Ltd.", "Magic Apparels Ltd./Red Button", 
    "Mamsy", "ILERI GIYIM SAN VE DIS TIC.STI.", "Zhejiang Sungin Industry Group co.,Ltd.", "Playshoes GmbH", "Rosemunde Aps", 
    "Society of Lifestyle APS", "Noman Terry Towel Mills Limited", "Ningbo China-Blue Fashion Co., Ltd", "All Sport N.V.", 
    "Brands4kids A/S", "Sweatertech Ltd.", "Brands of Scandinavia A/S", "CWF Children Worldwide Fashion", "Nore Tekstil Sanayi Ve Ticaret Ltd.Sti.", 
    "KM Apparel Knit (Pvt) Ltd.", "Udare Ltd.", "CKS Fashion", "King Wings International Trading Co., Ltd", "A.R.W. NV", 
    "Ningbo Leader Garment Co. LTD", "VERVALLEN Focus International Ltd", "W.S. Engross APS", "Sona Enterprises", "Renfold Ltd", 
    "CECEBA Group GmbH", "Sandgaard A/S", "TB International GmbH", "Kam International", "Haddad Brands Europe", "Urban Brands ApS", 
    "Bruuns Bazaar A/S", "WDKX Ltd ($)", "Mads Norgaard - Copenhagen A/S", "Tamara Tekstil Sanayi ve Dış Ticaret Ltd Sti", 
    "GANT DACH GmbH", "Regatta ltd.", "Mat Fashion", "Sportswear Company s.p.a.(Stone Island)", "New Visions Berlin GmbH", 
    "Stone Island Distribution S.R.L.", "VAUDE SPORT GmbH & Co. KG", "Lexson Brands B.V.", "J Carter Sporting Club Ltd - Castore", 
    "Nudie Jeans Marketing AB", "Marc O'Polo International GmbH", "M.G. Ekkelboom B.V.", "Liu Jo Benelux N.V.", "Treasure Will Limited", 
    "And Bording Aps", "Choice Clothing co. PVT.LTD", "Sports Group Denmark A/S", "NÜ A/S / Zoey", "Westex India", 
    "Shiv Shakti Home", "AycemTekstil San.Tic. Ltd.ŞTİ.", "Olymp Bezner Kg", "Rugs Creation", "ISYGS", "Pacific Jeans Ltd.", 
    "Medico sports fashion GmbH", "PNG Textiles Pvt Ltd", "Brave kid SRL", "Grimmer & Sommacal", "Felina gmbh", 
    "Hangzhou Bestsino Imp & Exp Co Ltd", "F&H of Scandinavia A/S", "Sharda Exports", "Laaj International", "Jai Knits Creations", 
    "Woolrich Europe S.p.A", "Bestwin (Shanghai) Home Fashion Ltd", "Didriksons Deutschland GmbH", "The Trade Aid Company", 
    "Peninsula Fashion Knitwear Limited", "Sovedam", "Ever-Glory Int. Group Apparel inc.", "Zaber & Zubair Fabrics Ltd", 
    "WDKX Ltd. (€)", "Yab Yum Clothing Co. Aps", "HTS Textilvertriebs GmbH", "DIESEL SPA", "Industrias Plasticas IGOR", 
    "Fashion Club 70 N.V.", "Levi's Footwear & Accessories (Switzerland) SA", "ParkStor Tekstil Tic.Ltd.Sti.", "Sumec Textile & Light Industry Co Ltd", 
    "Liewood A/S", "Coram DIY NV", "L.C. Jordan Company Limited", "Norban Comtex Ltd.", "Brand Machine International Limited"
]

filtered_material_weight_summary_df = material_weight_summary_df.filter(
    F.col("mcc_3_name").isin(mcc_3_names) & F.col("preferred_supplier_name").isin(suppliers)
)


# Display the filtered DataFrame
display(filtered_material_weight_summary_df)
total_net_despatch = filtered_material_weight_summary_df.groupBy().sum("net_despatch_qty").collect()[0][0]
display(total_net_despatch)


total_weight = filtered_material_weight_summary_df.groupBy().sum("Total_Weight").collect()[0][0]
display(total_weight)

