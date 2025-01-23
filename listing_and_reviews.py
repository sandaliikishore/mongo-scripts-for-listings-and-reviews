from pymongo import MongoClient
from datetime import datetime

# MongoDB connection setup
connection_string = "mongodb://localhost:27017/"
client = MongoClient(connection_string)

# Connect to the database and list collections
db = client["mongoPractise"]
collections = db.list_collection_names()
# print("Collections in the database:", collections)

# Access the 'movies' collection
collection = db['Listings&Reviews']
# print(db)

pipeline = [

#     # 1. Find the price per night of the first record in the listingsAndReviews collection.

#     {'$project' : {'price': 1}}, {'$limit':1}

#     # 2. Retrieve the cleaning fee of the first record in the listingsAndReviews collection.

#     {'$project': {'cleaning_fee':1}},{'$limit':1}

#     # 3. Find the host_name, host_location, host_about of the first record in the listingsAndReviews collection.

#       {'$project': 
#        {'host_name': '$host.host_name', 
#                     'host_location':'$host.host_location', 
#                     'host_about': '$host.host_about'}},
#                     {'$limit':1}

#     # 4. Retrieve the number of bedrooms in the first record in the listingsAndReviews collection.

#     {'$project': {'bedrooms': 1}},
#                 {'$limit':1}

#     # 5. Retrieve the number of guests are included in the first record in the listingsAndReviews collection. 

#     {'$project': {'guests_included': 1}}, {'$limit':1}

#     # 6. Write a MongoDB query to check whether the host have a profile picture in the first record in the listingsAndReviews collection

#     {'$project': {'host_with_pfp': '$host.host_has_profile_pic'}},{'$limit':1}

#     # 7. Write a MongoDB query to check whether the host's identity have been verified 
#     # in the first record in the listingsAndReviews collection

#     {'$project': {'host_identity_verification': '$host.host_identity_verified'}},{'$limit':1}

#     # 8. Write a MongoDB query to find how many listings does the host have in the 
#     # first records in the listingsAndReviews collection. 
    
#     {'$project':{'host_total_listing': '$host.host_total_listings_count'}},{'$limit':1}

#     # 9. Write a MongoDB query to find the street address of the first record in the listingsAndReviews collection. 

#     {'$project': {'street_address': '$address.street'}},{'$limit':1}

#     # 10. Find all the listings in the listingsAndReviews collection where the property_type field is set to "House"

#     {'$match': {'property_type': 'House'}}

#     # 11. Find all the listings in the listingsAndReviews collection with listing_url, name, host_name, host_location, 
#     # reviewer_name and price that have a nightly price greater than $500. 
    
#     {'$unwind': '$reviews'},
#     {'$match': {'price': {'$gt':500}}}, 
#     {'$project': {
#       'listing_url':1,
#       'name': 1,
#       'host_name': '$host.host_name',
#       'host_location': '$host.host_location',
#       'reviewer_name': '$reviews.reviewer_name',
#       'price':1
#       }
#      }
    
#     # 12. Find all the listings in the listingsAndReviews collection that are located in Brazil 
#     # and have a review score rating of at least 9. Return name, address, and review_scores_rating. 

#     {'$match':  {
#         'address.country': 'Brazil', 
#         'review_scores.review_scores_rating': {'$gte': 9}}
#     },
#     {'$project': {
#         'address': 1,
#         'name':1,
#         'review_rating':  '$review_scores.review_scores_rating'

#     }}

#     # 13. Find all the listings with name, address, reviewer_name, and review_scores_rating 
#     # in the listingsAndReviews collection that have a "hot tub" amenity and are located in the United States.

#     {'$match': {
#         'amenities': {'$in':['Hot tub']},
#          'address.country':'United States' }},
#     {'$unwind': '$reviews'},
#     {'$project': {
#         'name':1,
#         'address':1,
#         'reviewer_name': '$reviews.reviewer_name',
#         'review_rating':  '$review_scores.review_scores_rating'

#     }}

#     # 14. Find all the listings with name, amenities and price in the listingsAndReviews 
#     # collection that have a "pool" amenity and a nightly price between $200 and $400.
     
#     {'$match': {
#         'amenities': {'$in': ['Pool']},
#         'price': {'$gte':200, '$lte':400}
#     }},
#     {'$project':{
#         'name':1,
#         'price':1,
#         'amenities':1
#     }}

#     # 15. Find all the listings with name, amenities and address in the listingsAndReviews collection 
#     # that have a "Washer" amenity and are located in either Canada or Mexico.

#     {'$match': {
#         'amenities': {'$in': ['Washer']},
#         'address.country':{'$in': ['Canada', 'Mexico']}
#     }},
#     {'$project':{
#         'name':1,
#         'address':1,
#         'amenities':1        
#     }} 

#     # 16. Find the top 10 most reviewed listings with listing_url, 
#     # name, country, review_scores in the listingsAndReviews collection. 

#         {'$group': {'_id': {
#                 'listing_url': '$listing_url',
#                 'name': '$name',
#                 'country': '$address.country',
#                 'review_scores': '$review_scores'},
#         'count': {'$sum':1}}},

#         {'$sort': {'count':-1}},
#         {'$limit':10},
#         {'$project':{
#             '_id':0,
#             'listing_url':'$_id.listing_url',
#             'name': '$_id.name',
#             'country': '$_id.country',
#             'review_score': '$_id.review_scores',
#             'count':1
#         }}

#     # 17. Find all the listings with listing_url, name, address and review_scores in the listingsAndReviews collection that have a
#     #  "fireplace" amenity and a review score rating of at least 8. 

#     {'$match': {
#         'amenities': 'Essentials',
#         'review_scores.review_scores_rating': {'$gte': 8}}},

#         {'$project': {
#             '_id': 0,
#             'listing_url': 1,
#             'name': 1,
#             'address': 1,
#             'review_scores': 1            

#         }}
    
#     # 18. Find all the listings with listing_url, name, address and amenities, review scores in the listingsAndReviews collection 
#     # that have a "washer" amenity and are located in either Italy or Spain.
     
#     {'$match': {
#         'amenities': {'$in': ['Washer']},
#         'address.country':{'$in': ['Italy', 'Spain']}
#     }},
#     {'$project':{
#         'listing_url':1,
#         'name':1,
#         'address':1,
#         'amenities':1 ,
#         'review_scores': 1       
#     }} 

#     # 19. Find the listings with listing_url, name, address and 
#     # amenities, price, review scores in the listingsAndReviews collection that have the highest nightly prices. 

#     {'$project':{
#         'listing_url': 1,
#         'name': 1,
#         'address': 1,
#         'amenities': 1,
#         'price': 1,
#         'review_scores':1
#     }},
#     {'$sort': {'price':-1}},
#     {'$limit':1}

#     # 20. Find the listings with listing_url, name, address and amenities,price,review scores
#     #  in the listingsAndReviews collection that have the lowest nightly prices. 
    
#     {'$project':{
#         'listing_url': 1,
#         'name': 1,
#         'address': 1,
#         'amenities': 1,
#         'price': 1,
#         'review_scores':1
#     }},
#     {'$sort': {'price': 1}},
#     {'$limit':1}

#     # 21. Retrieve all documents with name, address, reviewer_name, review_scores_ratingin the 
#     # listingsAndReviewscollection that have a number_of_reviews field is equal to 0. 

#            {'$match': {'number_of_reviews': 0}}, 
#            {'$project': {
#                 'name':1,
#                 'address':1,
#                 'reviewer_name': 1,
#                 'review_scores_rating':1}

#             }

#     # 22. Retrieve all documents with name, address, host, reviewer_name, review_scores_ratingin 
#     # the listingsAndReviews collection where the host_is_superhost field is equal to true.


#     {'$unwind': '$reviews'},
#     {'$match': {'host.host_is_superhost': True}}, 
#     {'$project': {
#             'name':1,
#             'address':1,
#             'reviewer_name': '$reviews.reviewer_name',
#             'review_scores_rating':'$review_scores.review_scores_rating'}}


#     # 23. Retrieve all documents with name, address, host, reviewer_name, review_scores_ratingin 
#     # the listingsAndReviews collection where the coordinates field is not null.  

#     {'$unwind': '$reviews'},
#     {'$match': {'address.location.coordinates': {'$ne': None}}}, 
#     {'$project': {
#             'name':1,
#             'address':1,
#             'reviewer_name': '$reviews.reviewer_name',
#             'review_scores_rating':'$review_scores.review_scores_rating'}}

#     # 24. Retrieve all documents with name, address, host, bed_type, bed, review_scores_ratingfrom the
#     #  listingsAndReviewscollection where the beds field is greater than or equal to 2. 

        
#         {'$match': {'beds': {'$gte': 2}}}, 
#         {'$project': {
#             'name':1,
#             'address':1,
#             'host': 1,
#             'bed_type':1,
#             'beds':1,
#             'review_scores_rating':'$review_scores.review_scores_rating'}}



#     # 25. Find all listings with name, address, host in the listingsAndReviews collection 
#     # that have a host with a host_name containing the word "Livia".
#     #  
#         {'$match': {'host.host_name': {'$regex': 'Livia', '$options': 'i'}}}, 
#         {'$project': {
#             'name':1,
#             'address':1,
#             'host': 1,
#              }}


#     # 26. Find all listings with name, address, host in the listingsAndReviews collection
#     #  that have a host with a host_location of "Brazil". 

#         {'$match': {'host.host_name': {'$regex': 'Livia', '$options': 'i'}}}, 
#         {'$project': {
#             'name':1,
#             'address':1,
#             'host': 1,
#              }}      


#     # 27. Retrieve all documents with name, address, host, availability in the listingsAndReviews 
#     # collection where the availability_365 field is greater than 300. 

#         {'$match': {'availability.availability_365': {'$gt': 300}}}, 
#         {'$project': {
#             'name':1,
#             'address':1,
#             'host': 1,
#             'availability':1
#              }}        



#     # 28. Retrieve all documents with listing_url, name, bedrooms, pricein the listingsAndReviews
#     #  collectionwhere the bedrooms field is equal to 1. 

#         {'$match': {'bedrooms': 1}}, 
#         {'$project': {
#             'name':1,
#             'listing_url':1,
#             'price':1,
#             'bedrooms': 1,
#              }}    


#     # 29. Retrieve all documents with listing_url, name, bedrooms, cleaning_fee, and price in the 
#     # listingsAndReviews collection where the cleaning_fee field is not null. 

#         {'$match': {'cleaning_fee': {'$ne': None}}}, 
#         {'$project': {
#         'listing_url':1,
#             'name':1,
#             'price':1,
#             'bedrooms': 1,
#             'cleaning_fee':1
#              }}     


#     # 30. Retrieve all documents with listing_url, name, bedrooms, pricein the listingsAndReviews 
#     # collection where the price field is between 600 and 900. 

#         {'$match': {'price': {'$lte': 900 ,'$gte': 600 }}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'price':1,
#             'bedrooms': 1,
#             'cleaning_fee':1
#              }}

#     # 31. Retrieve all documents with listing_url, name, host, price in the 
#     # listingsAndReviews collection where the host_verifications array contains "email".

#         {'$match': {'host.host_verifications': 'email'}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'host':1,
#             'price':1
#              }}

#     # 32. Retrieve all documents with listing_url, name, amenity, host in the 
#     # listingsAndReviews collection where the amenities array contains both "TV" and "Wifi". 

#         {'$match': {'amenities': {'$all': ['TV', 'Wifi']}}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'host':1,
#             'amenities':1
#              }}

#     # 33. Find all listings with listing_url, name, amenities, host in the 
#     # listingsAndReviewscollection that have a host with a Jumio verification and a about section. 

#         {'$match': {'host.host_verifications': 'jumio', 'host.host_about': {'$exists': True, '$ne': ''}}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'host':1,
#             'amenities':1,
#             'host_about': '$host.host_about'
#              }}

#     # 34. Retrieve all documents with listing_url, name, host, price in the listingsAndReviews 
#     # collection where the host_total_listings_count field is greater than 1.

#         {'$match': {'host.host_total_listings_count':  {'$gt': 1}}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'host':1,
#             'price':1,

#              }} 

#     # 35. Retrieve all documents with listing_url, name, property_type,
#     # bed, price in the listingsAndReviewscollectionwhere the property_type field is equal to "Apartment" 
#     # and the beds field is greater than or equal to 2. 

#         {'$match': {'property_type': 'Apartment' , 'beds': {'$gte':2}}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'property_type':1,
#             'beds':1,
#             'price':1,

#              }} 

#     # 36. Find all listings with listing_url, name, property_type, bed, bathrooms, price in the
#     #  listingsAndReviews collection that have a minimum of 2 bathrooms.

#         {'$match': {'bathrooms':  {'$gte':2}}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'property_type':1,
#             'beds':1,
#             'price':1,
#             'bathrooms':1

#              }}  
    
#     #37. Find all listings with listing_url, name, property_type, bed, price, guests_included in the 
#     # listingsAndReviews collection that have a maximum of 5 guests included in the price.

#         {'$match': {'guests_included':  {'$lte':2}}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'property_type':1,
#             'beds':1,
#             'price':1,
#             'guests_included':1

#              }}  


#     # 38. Find all listings with listing_url, name, property_type, 
#     # bed, price, security_deposit in the listingsAndReviews collection that have a 
#     # price greater than $500 and a security deposit of $1000 or more. 

#         {'$match': {'price': {'$gt': 500},'security_deposit':  {'$gte':1000}}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'property_type':1,
#             'beds':1,
#             'price':1,
#             'security_deposit':1

#              }}  

#     # 39. Find all listings with listing_url, name, property_type, bed, price, cancellation_policy in the 
#     # listingsAndReviews collection that have a cancellation policy of "flexible".

#         {'$match': {'cancellation_policy':  "flexible"}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'property_type':1,
#             'beds':1,
#             'price':1,
#             'cancellation_policy':1

#              }}  

#     # 40. Find all listings with listing_url, name, property_type, bed_type, amenities, price in the listingsAndReviews 
#     # collection that have a real bed as the bed type and a kitchen amenity. 

#         {'$match': {'bed_type':  "Real Bed", 'amenities': 'Kitchen'}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'property_type':1,
#             'bed_type':1,
#             'price':1,
#             'amenities':1

#              }}  

#     # 41. Find all listings with listing_url, name, address, amenities in the listingsAndReviews 
#     # collection that have a 24-hour check-in amenity and are located in Brazil. 

#         {'$match': {'amenities':  "24-hour check-in", 'address.country': 'Brazil'}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'address':1,
#             'amenities':1

#              }}  

#     # 42. Find all listings with listing_url, name, address, reviews in the listingsAndReviews 
#     # collection that have at least one review. 

#         {'$match': {'reviews':  {'$exists' : True , '$not': {'$size': 0}}}}, 
#         {'$project': {
#             'listing_url':1,
#             'name':1,
#             'address':1,
#             'reviews':1

#              }} 

#     # 43. Find the number of documents that have a blank medium picture url in the istingsAndReviews collection.

#         {'$match': {'images.medium_url': ""}},{'$count': 'blank_medium_url' }

#     # 44. Find all listings with listing_url, name, address, availability_30 in the istingsAndReviews collection 
#     # that have an availability of at least 30 days. 

#     {'$match': {'availability.availability_30': {'$exists': True},'availability.availability_30': {'$gte': 30} }},
#     {'$project': {
#         'listing_url':1,
#         'name':1,
#         'address':1,
#         'availability.availability_30':1
#     }}

#     #45. Find all listings with listing_url, name, address in the listingsAndReviews collection that have a suburb of "Lagoa". 
#     {'$match': {'address.suburb': {'$exists': True},'address.suburb': 'Lagoa' }},
#     {'$project': {
#         'listing_url':1,
#         'name':1,
#         'address':1
#     }}


#     # 46. Find all listings with listing_url, name, address, host in the listingsAndReviews collection that have a host who 
#     # is a superhost and has at least 2 listings.
#     {'$match': {'host.host_is_superhost': True, 'host.host_total_listings_count': {'$gte': 2} }},
#     {'$project': {
#         'listing_url':1,
#         'name':1,
#         'address':1,
#         'host':1
#     }} 

#     # 47. Find all listings with listing_url, name, address, host in the listingsAndReviews collection that have a host
#     #  who has a profile pic and has been identity verified.

#     {'$match': {'host.host_id': {'$exists': True}, 'host.host_has_profile_pic': True,'host.host_identity_verified': True }},
#     {'$project': {
#         'listing_url':1,
#         'name':1,
#         'address':1,
#         'host':1
#     }}  

#     # 48. Write a  mongodb query to find the listing_url, name, address, host_verifications, and size of host_verification 
#     # under the host subdocument in the listingsAndReviews collection. 

#     {'$project': {
#         'listing_url':1,
#         'name':1,
#         'address':1,
#         'host_verifications':'host.host_verifications',
#         'host_verification_count': {'$size': '$host.host_verifications'}
#     }} 

#     # 49. Find all listings with listing_url, name, address, host_verificationand size of host_verification 
#     # array in the listingsAndReviews collection that have a host with at least 3 verifications.

#     {'$match': {'host.host_verifications': {'$exists': True}, 
#                 '$expr': {'$gte': [{'$size': '$host.host_verifications'},3] }}},
#     {'$project': {
#         'listing_url':1,
#         'name':1,
#         'address':1,
#         'host_verifications':'host.host_verifications',
#         'host_verification_count': {'$size': '$host.host_verifications'}
#     }} 

#     # 50. Find all listings with listing_url, name, address, host_picture_url in the listingsAndReviews 
#     # collection that have a host with a picture url.  

#     {'$match': {'host.host_has_profile_pic': {'$exists': True},'host.host_has_profile_pic': True}},
#     {'$project': {
#         'listing_url':1,
#         'name':1,
#         'address':1,
#         'host_pfp': '$host.host_picture_url'
#     }}
]

results = list(collection.aggregate(pipeline))

# Print each result
for result in results:
    print(result)
# ----------------------------------------------------------------------------------------------------------------------------------------
