import pymongo

#Show the number of the memes per year in descending order (SQL)

db.memes.find().sort(memes:1) 
#select from table order by memes, group by, siis  .group()

#aggregates
db.memes.aggregate({$group:
                                 {memes, totalquantity:
                                  {$count: memes: -1}  #desc order
                                  }
                                 }
                                )

memes_per_year = db.memes.find({}, {})


#teine variant
memes_per_year=db.memes.aggregate([
                              {"$group" : {_id:"$memes", count:{$sum:1}}},
                              {$sort: {count: -1}}
])

#kolmas variant- peaks olema õige 
memes_per_year=db.memes.aggregate(["$group": {_id:{ year:"$year"}, count:{$sum:1}}},
                             {$sort:{"count": -1}}
])

print(memes_per_year)

#neljas variant 
pipeline = [
            {"$group": {_id:{memes: "memes"}, count: {$sum:1}}},
            {$sort: {"count:-1"}},
            {"$match": {
                "year"}
            }
]

results = db.memes.aggregate(pipeline)



# Which memes have the largest number of common tags?
number_of_common_tags = df.memes.collection.aggregate([
                                      {$group : {_id:"$tags", count:{$sum:1}}},
                                      {$sort: {count:-1}}
])




# All memes containing tag Estonia. Sort by year.

df.memes.find({_id: memes:"*Estonia*"}, year:-1)  # see vale

#teine variant 
df.memes.find({"$text" :{"search" : "Estonia \"estonia\""} }, {"_id" : 0})


#kolmas variant - see võiks olla õige

memes_containing_tag_Estonia =df.memes.find({"$text" :{"search" : "Estonia \"estonia\""}})
print(memes_containing_tag_Estonia)

# Rank memes by origin (youtube, 4chan, fb etc) (content>origin>links)


# Something to Query about semantic relations of memes 
