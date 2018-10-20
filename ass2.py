import requests
import pandas as pd
import json
import pymongo
import datetime as dt
from flask import Flask
from flask import request
from flask_restplus import Resource, Api
from flask_restplus import fields
from flask_restplus import inputs
from flask_restplus import reqparse
from bson import ObjectId
from bson import json_util
import re
import operator
import itertools
import math

app = Flask(__name__)
api = Api(app,
          default="Economic Indicator",  # Default namespace
          title="Indicator Dataset",  # Documentation Title
          description="This is just a simple example to show how publish data as a service.")  # Documentation Description

# The following is the schema of indicator
indicator_model = api.model('indicator', {
	'indicator_id': fields.String
})


@api.route('/economic_indicator')
class IndicatorList(Resource):
	@api.response(200, 'Successful')
	@api.doc(description="Get all indicators")
	def get(self):
		ret = []
		df = read_from_mongodb(db, collection)
		
		if df.empty:
			return ret
		else:
			for row in df.itertuples():
				collection_id = row[2]
				creation_time = row[3]
				indicator = row[5]
				dictionary = {"location": "/economic_indicator/{}".format(collection_id), "collection_id": "{}".format(collection_id), "creation_time": "{}".format(creation_time), "indicator": "{}".format(indicator)}
				ret.append(dictionary)
				
			return ret



	@api.response(200, 'OK')
	@api.response(201, 'Indicator Imported Successfully')
	@api.response(400, 'Validation Error')
	@api.response(404, 'indicator id doesnt exist in the data source')
	@api.doc(description="Add a new indicator")
	@api.expect(indicator_model, validate=True)
	
	def post(self):
		#columns = ['_id', 'indicator', 'indicator_value', 'creation_time', 'entries']
		#df = pd.DataFrame(columns=columns)
		df = read_from_mongodb(db, collection)
		indicator = request.json
		
		url = "http://api.worldbank.org/v2/countries/all/indicators/INDICATOR_ID?date=2012:2017&format=json&per_page=2000"

		if 'indicator_id' not in indicator:
			return {"message": "Missing indicator_id"}, 400
		for key in indicator:
			if key not in indicator_model.keys():
				# unexpected column
				return {"message": "Indicator ID {} is invalid".format(key)}, 400

		indicator_id = indicator['indicator_id']

		# check if the given identifier does not exist
		if df.empty:
			new_indicator = create_new_dataframe(url, indicator_id)
			if new_indicator is None:
				return {"message": "indicator id {} doesn't exist in the data source".format(indicator_id)}, 404
			mongo_df = read_from_mongodb(db, collection)
			df = df.append(mongo_df)
		else:
			if indicator_id in df.indicator.values:
				new_row = df.loc[df['indicator'] == indicator_id]
				collection_id = new_row['collection_id'].values[0]
				creation_time = new_row['creation_time'].values[0]
				return {"location": "/economic_indicator/{}".format(collection_id), "collection_id": "{}".format(collection_id), "creation_time": "{}".format(creation_time), "indicator": "{}".format(indicator_id)}, 200
			else:
				new_indicator = create_new_dataframe(url, indicator_id)
				if new_indicator is None:
					return {"message": "indicator id {} doesn't exist in the data source".format(indicator_id)}, 404
				mongo_df = read_from_mongodb(db, collection)
				df = df.append(mongo_df)
		#print_dataframe(df)
		new_row = df.loc[df['collection_id'] == indicator_id]
		if new_row.empty:
			new_row = df.loc[df['indicator'] == new_indicator]
		#print(new_row)
		collection_id = new_row['collection_id'].values[0]
		creation_time = new_row['creation_time'].values[0]
		
		return {"location": "/economic_indicator/{}".format(collection_id), "collection_id": "{}".format(collection_id), "creation_time": "{}".format(creation_time), "indicator": "{}".format(new_indicator)}, 201
		
		
@api.route('/economic_indicator/<id>')
@api.param('id', 'collection id for indicator')
class Indicators(Resource):
	@api.response(404, 'Indicator was not found')
	@api.response(200, 'Successful')
	@api.doc(description="Get a indicator by its ID")
	def get(self, id):
		df = read_from_mongodb(db, collection)
		if df.empty:
			api.abort(404, "Indicator {} doesn't exist".format(id))
			
		try:
			if db[collection].find({"collection_id":id}).count() > 0:
					#indicator = df.loc[df['_id'] == ObjectId(id)]
					#print(indicator['indicator_value'])
					#print(type(indicator['indicator_value']))
				
				try:
					indicator_cur = db[collection].find({"collection_id":id})
					for values in indicator_cur:
						indicator = values
				
					dictionary = {}
					for values in indicator:
						#print(values)
						#print(type(values))
						#print(indicator[values])
						#print(type(indicator[values]))
						if "_id" in values:
							pass
						else:
							dictionary[values] = str(indicator[values])
					return dictionary
				except:
					api.abort(404, "Indicator {} doesn't exist".format(id))
				
			else:
				api.abort(404, "Indicator {} doesn't exist".format(id))
		except:
			api.abort(404, "Indicator {} doesn't exist".format(id))

	@api.response(404, 'Indicator was not found')
	@api.response(200, 'Successful')
	@api.doc(description="Delete a indicator by its ID")
	def delete(self, id):
		df = read_from_mongodb(db, collection)
		if df.empty:
			api.abort(404, "Indicator {} doesn't exist".format(id))
		try:
			if db[collection].find({"collection_id":id}).count() > 0:
				db[collection].remove({"collection_id":id})
				return {"message": "Collection = {} is removed from the database!".format(id)}, 200
			else:
				api.abort(404, "Indicator {} doesn't exist".format(id))
		except:
			api.abort(404, "Indicator {} doesn't exist".format(id))
		
parser = reqparse.RequestParser()
parser.add_argument('q')	
@api.route('/economic_indicator/<id>/<year>')
@api.param('id', 'collection id for indicator')
@api.param('year', 'year to get indicator for')
@api.param('q', 'query values for sort')
class Indicators(Resource):
	@api.response(404, 'Indicator was not found')
	@api.response(200, 'Successful')
	@api.expect(parser)
	@api.doc(description="Get a indicator by its ID, year and name of country")
	def get(self, id, year):
		query = parser.parse_args().get('q')
		if query is None:
			match = None
		else:
			match = re.match(r"([a-z]+)([0-9]+)$", query)
		
		df = read_from_mongodb(db, collection)
		if df.empty:
			api.abort(404, "Indicator {}, {} doesn't exist. DB Empty".format(id, year))
			
		try:
			if db[collection].find({"collection_id":id}).count() > 0:
				try:
					indicator_cur = db[collection].find({"collection_id":id})
					for values in indicator_cur:
						indicator = values

					indicator_id = indicator['indicator']
					indicator_value = indicator['indicator_value']
					entries = indicator['entries']
					years = []
					for dic in entries:
						if dic['date'] == year:
							years.append(dic)
					#print(years)
					for dics in years:
						if math.isnan(dics['value']):
							dics['value'] = -1
					new_list = sorted(years, key=lambda k: k['value'], reverse=True)
					#print(newlist)
					#entries_df = pd.DataFrame(entries)
					#year_df = entries_df.loc[entries_df['date'] == year]
					#if year_df.empty:
					#	api.abort(404, "Indicator with year = {} doesn't exist.".format(year))
					#print_dataframe(year_df)
					#dictionary = {}
					#new_list = list_of_dicts.sort(key=operator.itemgetter('name'))
				except:
					api.abort(404, "Indicator {}, {} doesn't exist.".format(id, year))
			else:
				api.abort(404, "Indicator {}, {} doesn't exist.".format(id, year))
		except:
			api.abort(404, "Indicator {}, {} doesn't exist.".format(id, year))
			
#Should have list of countries with _id and year in desc order in new_list

		if match:
			query_items = match.groups()
			n = int(query_items[1])
			if query_items[0] == 'top':
				if 1 <= n <= 100:
					result_countries = list(itertools.islice(new_list, n))
				else:
					result_countries = new_list
					
			elif query_items[0] == 'buttom':
				if 1 <= n <= 100:
					new_list = sorted(new_list, key=lambda k: k['value'])
					result_countries = list(itertools.islice(new_list, n))
				else:
					result_countries = new_list
			else:
				result_countries = new_list
		else:
			result_countries = new_list
			
		for dics in result_countries:
			dics['country'] = dics['country'].encode('latin-1', 'ignore').decode("utf-8")
		resp = {"indicator": "{}".format(indicator_id), "indicator_value": "{}".format(indicator_value), "entries": result_countries}
		
		return resp
		

@api.route('/economic_indicator/<id>/<year>/<country>')
@api.param('id', 'collection id for indicator')
@api.param('year', 'year to get indicator for')
@api.param('country', 'name of country to get indicator for')
class Indicators(Resource):
	@api.response(404, 'Indicator was not found')
	@api.response(200, 'Successful')
	@api.doc(description="Get a indicator by its ID, year and name of country")
	def get(self, id, year, country):
		df = read_from_mongodb(db, collection)
		if df.empty:
			api.abort(404, "Indicator {}, {}, {} doesn't exist. DB Empty".format(id, year, country))
			
		try:
			if db[collection].find({"collection_id":id}).count() > 0:
					#indicator = df.loc[df['_id'] == ObjectId(id)]
					#print(indicator['indicator_value'])
					#print(type(indicator['indicator_value']))
				
				try:
					indicator_cur = db[collection].find({"collection_id":id})
					for values in indicator_cur:
						indicator = values
						
					indicator_id = indicator['indicator']
					entries = indicator['entries']
					entries_df = pd.DataFrame(entries)
					#print(country, year)
					country_df = entries_df.loc[entries_df['country'] == country]
					if country_df.empty:
						api.abort(404, "Indicator with country = {} doesn't exist.".format(country))
					year_df = country_df.loc[country_df['date'] == year]
					#print_dataframe(year_df)
					if year_df.empty:
						api.abort(404, "Indicator with year = {} doesn't exist.".format(year))
					
					resp = {"collection_id": "{}".format(id), "indicator": "{}".format(indicator_id), "country": "{}".format(year_df['country'].values[0]), "year": "{}".format(year_df['date'].values[0]), "value": "{}".format(year_df['value'].values[0])}
						
					return resp
				except:
					api.abort(404, "Indicator {}, {}, {} doesn't exist.".format(id, year, country))
				
			else:
				api.abort(404, "Indicator {}, {}, {} doesn't exist.".format(id, year, country))
		except:
			api.abort(404, "Indicator {}, {}, {} doesn't exist.".format(id, year, country))
			


		
		


def print_dataframe(dataframe, print_column=True, print_rows=True):
    # print column names
    if print_column:
        print(",".join([column for column in dataframe]))

    # print rows one by one
    if print_rows:
        for index, row in dataframe.iterrows():
            print(",".join([str(row[column]) for column in dataframe]))

def create_new_dataframe(url, indicator_id):
	url = url.replace('INDICATOR_ID', indicator_id)
	json_obj = get_json(url)
	new_df = json_to_dataframe(json_obj)
	if new_df is None:
		return None
	else:
		write_in_mongodb(new_df, db, collection, indicator_id)
		return new_df['indicator'].values[0]

def get_json(url):
    """
    :param url: RUL of the resouce
    :return: JSON
    """
    resp = requests.get(url=url)
    data = resp.json()
    return data


def json_to_dataframe(json_obj):
    """
    Please Open the JSON using the given URL to be familiar with the
    structure of the expected JSON object

    :param json_obj: JSON object for the dataset
    :return: A dataframe
    """
    try:
    	json_data = json_obj[1]
    except IndexError:
    	return None
    
    new_data = pd.DataFrame(data=json_data)
    
    creation_time = dt.datetime.now()
    #collection_id = 999
    indicator = new_data['indicator'][0]
    indicator_id = indicator['id']
    indicator_value = indicator['value']
    
    entries = []
    
    for index, row in new_data.iterrows():
    	entry_dic = {'country': row['country']['value'], 'date':row['date'], 'value':row['value']}
    	entries.append(entry_dic)
    	
    columns = ['indicator', 'indicator_value', 'creation_time', 'entries']
    data = {'indicator': indicator_id, 'indicator_value': indicator_value, 'creation_time': creation_time, 'entries': entries}
    
    result = pd.DataFrame(columns=columns)
    result.loc[0] = data

    return result
    
def reset_mongodb(db, collection):
	"""
	:param db: The name of the database
	:param collection: the name of the collection inside the database
	"""
	c = db[collection]
	x = c.delete_many({})

	print(x.deleted_count, " documents deleted.")

def write_in_mongodb(dataframe, db, collection, indicator_input):
    """
    :param dataframe: 
    :param db: The name of the database
    :param collection: the name of the collection inside the database
    :param indicator_input: indicator_input
    """
    c = db[collection]
    
    creation_time = dt.datetime.now()
    collection_id = indicator_input
    indicator_id = dataframe['indicator'].values[0]
    indicator_value = dataframe['indicator_value'].values[0]
    
    entries = dataframe['entries'].values[0]
    
    c.insert({'collection_id': collection_id, 'indicator': indicator_id, 'indicator_value': indicator_value, 'creation_time': creation_time, 'entries': entries})


def read_from_mongodb(db, collection):
    """
    :param db: The name of the database
    :param collection: the name of the collection inside the database
    :return: A dataframe which contains all documents inside the collection
    """
    c = db[collection]
    return pd.DataFrame(list(c.find()))


if __name__ == '__main__':
	#Set up mongodb from mlab
	uri = 'mongodb://z5061885:assignment2@ds253922.mlab.com:53922/comp9321_ass2'
	client = pymongo.MongoClient(uri)
	db = client['comp9321_ass2']
	collection = 'bank_indicator'
	
	print("Reset mongodb")
	reset_mongodb(db, collection)
	
	#print("Querying the database")
	#df = read_from_mongodb(db, collection)

	# run the application
	app.run(debug=True)
	
	client.close()
