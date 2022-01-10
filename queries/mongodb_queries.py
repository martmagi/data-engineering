import pymongo

MONGO_USER = 'data-engineering-g12'
MONGO_SECRET = 'password'
MONGO_URL = 'data-engineering-g12.wvade.mongodb.net'
MONGO_SCHEMA = 'myFirstDatabase'
MONGO_CONNECTION_STRING = f'mongodb+srv://{MONGO_USER}:{MONGO_SECRET}@{MONGO_URL}/{MONGO_SCHEMA}?retryWrites=true&w=majority'

client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
db = client.memesDB

#Show the number of the memes per year in descending order (SQL)
memes_per_year_pipeline = [
    {
        "$group": {
            "_id": "$year",
            "memes_per_year": {
                "$sum": 1
            }
        }
    },
    {
        "$sort": {
            "memes_per_year": -1
        }
    },
    {
        "$limit": 5
    }
]
memes_per_year = db.memes.aggregate(memes_per_year_pipeline)

print('Number of memes per year in descending order:')
for doc in memes_per_year:
    print(doc)
print('')

memes_containing_tag_finland = db.memes.find({"tags": "finland"})

print('Memes containing tag \'finland\':')
for doc in memes_containing_tag_finland:
    print(doc)
print('')

# Which memes have the largest number of common tags?
common_tags_pipeline = [
    {
        "$unwind": "$tags"
    },
    {
        "$group": {
            "_id": "$tags",
            "count": {
                "$sum": 1
            }
        }
    },
    {
        "$sort": {
            "count": -1
        }
    },
    {
        "$limit": 5
    }
]
number_of_common_tags = db.memes.aggregate(common_tags_pipeline)

print('Memes that have the largest number of common tags:')
for doc in number_of_common_tags:
    print(doc)
print('')

# Rank memes by origin (youtube, 4chan, fb etc) (content>origin>links)
memes_by_origin_pipeline = [
    {
        "$unwind": "$content.origin.links"
    },
    {
        "$group": {
            "_id": "$content.origin.links",
            "count": {
                "$sum": 1
            }
        }
    },
    {
        "$sort": {
            "count": -1
        }
    },
    {
        "$limit": 5
    }
]
memes_by_origin = db.memes.aggregate(memes_by_origin_pipeline)

print('Rank memes by origin:')
for doc in memes_by_origin:
    print(doc)
print('')

# Something to Query about semantic relations of memes

#Show the number of the memes per weekday in descending order (SQL)
memes_per_weekday_pipeline = [
    {
        "$group": {
            "_id": "$added_weekday",
            "memes_per_weekday": {
                "$sum": 1
            }
        }
    },
    {
        "$sort": {
            "memes_per_weekday": -1
        }
    },
    {
        "$limit": 3
    }
]
memes_per_weekday = db.memes.aggregate(memes_per_weekday_pipeline)
print('Number of memes per weekday in descending order:')
for doc in memes_per_weekday:
    print(doc)
print('')

#Show the number of the memes per month in descending order (SQL)
memes_per_month_pipeline = [
    {
        "$group": {
            "_id": "$added_month",
            "memes_per_month": {
                "$sum": 1
            }
        }
    },
    {
        "$sort": {
            "memes_per_month": -1
        }
    },
    {
        "$limit": 6
    }
]
memes_per_month = db.memes.aggregate(memes_per_month_pipeline)
print('Number of memes per month in descending order:')
for doc in memes_per_month:
    print(doc)
print('')
