###################################################
# INFSCI 2725: Data Analytics                     #
# Assignment 1: Storage and processing of data    #
# Coded by Mohammed Alharbi & Yixuan Edison Wang  #
###################################################

import os.path
from pymongo import MongoClient
from collections import OrderedDict
from datetime import datetime

# ==================================================================== #
# /// Data Files and this code file have to be in the SAME FOLDER  /// #
# ==================================================================== #

file_name = ''
client = MongoClient()
# connect to the movies database, if 'moviedb' doesn't exist, it'll be created automatically.
db = client['moviesdb']
# connect to the movies collection, if 'movies' collection doesn't exist, it'll be created automatically.
movies_collection = db.movies
count_movies, count_tags, count_ratings = 0, 0, 0

# ====================================================================================================== #
# === You may Refer to Movies Document Plan that we applied in MovieDocPlan.txt to better understand === #
# ====================================================================================================== #

# Step #1: Insert movies.dat into MongoDb
file_name = 'movies.dat'
if(os.path.exists(file_name)):
	with open(file_name) as f:
		# read the data file line by line
		for line in f:
			# Remove the end-line character, and split the line by ::
			values = line.replace('\n', '').split('::')
			
			# Use a specific dictionary to maintain the order of data
			doc = OrderedDict([('MovieID', int(values[0])), ('Title', values[1]), ('Genres', values[2].split('|'))])
			# insert the document into movie collection
			movies_collection.insert_one(doc)
			count_movies += 1

			print(str(count_movies) + ' movies added.')

		# Step #2: Embed tags.dat into Movie Collection
		file_name = 'tags.dat'
		if(os.path.exists(file_name)):
			with open(file_name) as f:
				# read the data file line by line
				for line in f:
					# Remove the end-line character, and split the line by ::
					values = line.replace('\n', '').split('::')
					
					# Use a specific dictionary to maintain the order of data
					doc = OrderedDict([('UserID', int(values[0])), ('Tag', values[2]), ('Timestamp', datetime.utcfromtimestamp(float(values[3])))])
					# update movie collection by embeding a new document in Tags field
					movies_collection.update({'MovieID': int(values[1])}, {'$push': {'Tags': doc}}, upsert=True)
					count_tags += 1
					
					print(str(count_tags) + ' tags added.')

		else:
			print('Either ' + file_name + ' is missing or is not readable.')

		# Step #3: Embed ratings.dat into Movie Collection
		file_name = 'ratings.dat'
		if(os.path.exists(file_name)):
			with open(file_name) as f:
				# read the data file line by line
				for line in f:
					# Remove the end-line character, and split the line by ::
					values = line.replace('\n', '').split('::')
					
					# Use a specific dictionary to maintain the order of data
					doc = OrderedDict([('UserID', int(values[0])), ('Rating', float(values[2])), ('Timestamp', datetime.utcfromtimestamp(float(values[3])))])
					# update movie collection by embeding a new document in Ratings field
					movies_collection.update({'MovieID': int(values[1])}, {'$push': {'Ratings': doc}}, upsert=True)
					count_ratings += 1
					
					print(str(count_ratings) + ' ratings added.')
		else:
			print('Either ' + file_name + ' is missing or is not readable')

else:
	print('Either ' + file_name + ' is missing or is not readable.')

# Print Summary
count = movies_collection.count()
count_tags = list(movies_collection.aggregate([ { '$unwind': "$Tags" }, { '$group': { '_id': None, 'count': {'$sum': 1} } } ]))[0]['count']
count_ratings = list(movies_collection.aggregate([ { '$unwind': "$Ratings" }, { '$group': { '_id': None, 'count': {'$sum': 1} } } ]))[0]['count']

print('--------------------------------------------------------------')
print('     Total Documents that Exist Currently in the Database')
print('--------------------------------------------------------------')
print('   Movies: ' + str(count))
print('     Tags: ' + str(count_tags))
print('  Ratings: ' + str(count_ratings))
print('--------------------------------------------------------------')
