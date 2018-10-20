
#!/usr/bin/python3
import json

import pandas as pd 
from flask import Flask
from flask import request
from flask_restplus import Resource, Api
from flask_restplus import fields
from flask_restplus import inputs
from flask_restplus import reqparse

app = Flask(__name__)
api = Api(app)

book_model = api.model('Book', {
	'Flickr_URL': fields.String,
	'Publisher': fields.String,
	'Author': fields.String,
	'Title': fields.String,
	'Date_of_Publication': fields.Integer,
	'Identifier': fields.Integer,
	'Place_of_Publication': fields.String
})

parser = reqparse.RequestParser()
parser.add_argument('order', choices=list(column for column in book_model.keys()))
parser.add_argument('ascending', type=inputs.boolean)

@api.route('/books/')
class BooksList(Resource):
	@api.expect(parser, validate=True)
	def get(self):
		args = parser.parse_args()

		order_by = args.get('order')
		ascending = args.get('asending', True)

		if order_by:
			df.sort_values(by=order_by, inplace=True, ascending=ascending)

		json_str = df.to_json(orient='records')

		ret = json.loads(json_str)
		return ret

	@api.expect(book_model)
	def post(self):
		book = request.json

		if 'Identifier' not in book:
			return {"message": "Missing Identifier"}, 400

		id = book['Identifier']

		if id in df.index:
			return {"message": "A book with Identifier={} is already in the dataset".format(id)}, 400

		for key in book:
			if key not in book_model.keys():
				return {"message": "Property {} is invalid".format(key)}, 400
			df.loc[id, key] = book[key]

			df.append(book, ignore_index=True)
			return {"message": "Book {} is created".format(id)}, 201

@api.route('/books/<int:id>')
class Books(Resource):
	def get(self, id):
		if id not in df.index:
			api.abort(404, "Book {} doesn't exist".format(id))

		book = dict(df.loc[id])
		return book

	def delete(self, id):
		if id not in df.index:
			api.abort(404, "Book {} doesn't exist".format(id))

		df.drop(id, inplace=True)
		return {"message": "Book {} is removed.".format(id)}, 200

	@api.expect(book_model)
	def put(self, id):
		if id not in df.index:
			api.abort(404, "Book {} doesn't exist".format(id))

		book = request.json

		if 'Identifier' in book and id != book['Identifier']:
			return {"message": "Identifier {} cannot be changed".format(id)}, 400

		for key in book:
			if key not in book_model.keys():
				return {"message": "Property {} is invalid".format(id)}, 400
			df.loc[id, key] = book[key]

		df.append(book, ignore_index=True)
		return {"message": "Book {} has been successfully updated".format(id)}, 200

if __name__ == '__main__':
	columns_to_drop = ['Edition Statement', 'Corporate Author', 'Corporate Contributors', 'Former owner', 'Engraver', 'Contributors', 'Issuance type', 'Shelfmarks']
    
	csv_file = "Books.csv"
	df = pd.read_csv(csv_file)

	df.drop(columns_to_drop, inplace=True, axis=1)

	new_date = df['Date of Publication'].str.extract(r'^(\d{4})', expand=False)
	new_date = pd.to_numeric(new_date)
	new_date = new_date.fillna(0)
	df['Date of Publication'] = new_date

	df.columns = [c.replace(' ', '_') for c in df.columns]

	df.set_index('Identifier', inplace=True)

	app.run(debug=True)
