import polars as pl
import datetime as dt

df = pl.DataFrame(
    {
        "name": ["Alice Archer","Ben Brown","Chloe Cooper","Daniel Donovan"],
        "birthdate": [
            dt.date(1997,1,10),
            dt.date(1985,2,15),
            dt.date(1983,3,22),
            dt.date(1981,4,30),
        ],
        "weight": [57.9,72.5,53.6,83.1], #(kg)
        "height": [1.56,1.77,1.65,1.75], #(m)
    }
)

print(df)
#=============================================================================
#using select

result = df.select(
    pl.col("name"),
    pl.col("birthdate").dt.year().alias("birth_year"),
    (pl.col("weight")/(pl.col("height")**2)).alias("bmi"),
)
print(result)

#=============================================================================
#using expression expansion - manipulating 2 or more cols at once

result = df.select(
    pl.col("name"),
    (pl.col("weight","height")*0.95).round(2).name.suffix("-5%"),
)
print(result)

#==========================================================================
#Using with_columns - to add new columns
#Here a named expression was used instead of .alias

result = df.with_columns(
    birth_year = pl.col("birthdate").dt.year(),
    bmi = pl.col("weight")/(pl.col("height")**2),
)
print(result)

#===========================================================================
#Using filter - allows us to create a second dataframe with a subset of the rows of the original one:
#Here we're filtering people born before 1990

result = df.filter(pl.col("birthdate").dt.year()<1990)
print(result)

#You can also provide multiple predicate expressions as separate parameters, which is more convenient
#than putting them all together with &

result = df.filter(
    pl.col("birthdate").is_between(dt.date(1982,12,31),dt.date(1996,1,1)),
    pl.col("height") > 1.7,
)
print (result)

#===================================================================
#using group_by - to group together the rows of a dataframe that share the same value across one or more expressions.
#maintain_order forces polars to present the resulting groups in the same order as they appear in the og dataframe
result = df.group_by(
    (pl.col("birthdate").dt.year()//10*10).alias("decade"),
    maintain_order = True,
).len()
print(result)

#===================================================================
#Using aggregations such as SUM, COUNT, MIN, MAX etc.

result = df.group_by(
    (pl.col("birthdate").dt.year()//10*10).alias("decade"),
    maintain_order = True,
).agg(
    pl.len().alias("sample_size"),
    pl.col("weight").mean().round(2).alias("avg_weight"),
    pl.col("height").max().alias("tallest"),
)
print(result)

#=======================================================================
#MORE COMPEX QUERIES

result = (
    df.with_columns(
        (pl.col("birthdate").dt.year()//10*10).alias("decade"),
        pl.col("name").str.split(by=" ").list.first(),
    )
    .select(
        pl.all().exclude("birthdate"),
    )
    .group_by(
        pl.col("decade"),
        maintain_order = True,
    )
    .agg(
        pl.col("name"),
        pl.col("weight","height").mean().round(2).name.prefix("avg_"),
    )
)
print(result)

#==================================================================
#JOINING DATAFRAMES

df2 = pl.DataFrame(
    {
        "name":["Benedict Benjamin","Daniel Donovan","Alice Archer","Chloe Cooper"],
        "parent":[True, False, False, False],
        "siblings":[1, 2, 3, 4],
        "Nationality":["Tanzanian","Ugandan","Kenyan","Ethiopian"]
    }
)
print(df.join(df2, on="name", how="left"))

df3 = pl.DataFrame(
    {
        "name": ["Ethan Edwards", "Fiona Foster", "Grace Gibson", "Henry Harris"],
        "birthdate":[
            dt.date(1977, 5, 10),
            dt.date(1975, 6, 23),
            dt.date(1973, 8, 3),
        ],
        "weight":[67.9, 72.5, 57.6, 93.1], #(kg)
        "height": [1.76, 1.6, 1.66, 1.8], #(m)
    }
)
print(pl.concat([df, df3], how="vertical"))