
# IMPORTS 
import csv
import requests
from bs4 import BeautifulSoup

# CONSTANTS
BASE_URL = 'http://www.city-data.com/city/{}-{}.html'
ALL_STATES = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Loiusiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New-Hampshire', 'New-Jersey', 'New-Mexico', 'New-York', 'North-Carolina', 'North-Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode-Island', 'South-Carolina', 'South-Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West-Virginia', 'Wisconsin', 'Wyoming']
ALL_FILES = ['alabama.txt', 'alaska.txt', 'arizona.txt', 'arkansas.txt', 'california.txt', 'colorado.txt', 'connecticut.txt', 'delaware.txt', 'florida.txt', 'georgia.txt', 'hawaii.txt', 'idaho.txt', 'illinois.txt', 'indiana.txt', 'iowa.txt', 'kansas.txt', 'kentucky.txt', 'loiusiana.txt', 'maine.txt', 'maryland.txt', 'massachusetts.txt', 'michigan.txt', 'minnesota.txt', 'mississippi.txt', 'missouri.txt', 'montana.txt', 'nebraska.txt', 'nevada.txt', 'new_hampshire.txt', 'new_jersey.txt', 'new_mexico.txt', 'new_york.txt', 'north_carolina.txt', 'north_dakota.txt', 'ohio.txt', 'oklahoma.txt', 'oregon.txt', 'pennsylvania.txt', 'rhode_island.txt', 'south_carolina.txt', 'south_dakota.txt', 'tennessee.txt', 'texas.txt', 'utah.txt', 'vermont.txt', 'virginia.txt', 'washington.txt', 'west_virginia.txt', 'wisconsin.txt', 'wyoming.txt']
RACES = {'White alone': 'Pct_White', 'Hispanic':'Pct_Hispanic', 'Asian alone':'Pct_Asian', 'Two or more races':'Pct_Mixed', 'American Indian alone': 'Pct_Native American', 'Black alone':'Pct_Black', 'Native Hawaiian and OtherPacific Islander alone': 'Pct_Native Hawaiian/Pacific Islander', 'Other race alone':'Pct_Other'}
LEGAL_STATES = ['Alaska', 'Oregon', 'Washington', 'California', 'Nevada', 'Colorado', 'Massachusetts', 'Maine', 'Vermont']

STATE_CSV_COLNS = ['population', 'median_age', 'median_gross_rent', 'Pct_White', 'Pct_Hispanic', 'Pct_Mixed', 'Pct_Asian', 'Pct_Black', 'Pct_Native American', 'Pct_Other', 'mj_legal']
ERR_CSV_COLNS = ['state', 'url', 'error']
ERROR = (None, {'population': None, 'median_age': None, 'Pct_White': None, 'Pct_Hispanic': None, 'Pct_Mixed': None, 'Pct_Asian': None, 'Pct_Black': None, 'Pct_Native American': None, 'Pct_Other': None, 'median_gross_rent': None})
NUM_CITIES = 25149

# MAIN 
def generate_urls():
	url_list = []
	for f, s in zip(ALL_FILES, ALL_STATES):
		state_path = 'cities/{}'.format(f)
		with open(state_path, 'r') as state_cities:
			for city in state_cities.readlines():
				url_list.append(format_url(s, city))
	return (state_url for state_url in url_list)

def format_url(state, city):
	form_city = city.rstrip()
	form_city = '-'.join(form_city.split(' '))
	return (state, BASE_URL.format(form_city, state))

### SCRAPED SITE PARSE METHODS

def get_population(soup):
	pop = (soup.split(' ')[3]).replace(',', '')
	return {'population': int(pop)}

def get_median_age(state, soup):
	state_median_age = soup.split(state)[0]
	med_age = state_median_age.replace(u'\xa0', u' ').split(' ')
	for string in med_age:
		if string.replace('.', '', 1).isdigit():
			return {'median_age': float(string)}

def get_median_gross_rent(soup):
	rent = soup.split(' ')
	rent = rent[len(rent)-1].replace(',', '')
	rent = rent.replace('.', '')
	rent = rent.replace('$', '')
	return {'median_gross_rent': int(rent)}

def get_prop_races_dict(soup):
	all_races = [r.text for r in soup.find_all('b')]
	all_props = [p.text for p in soup.find_all(class_='badge alert-info')]
	all_props = [float(p.replace('%', '')) for p in all_props]

	races_dict = {}
	for r, p in zip(all_races, all_props):
		races_dict[RACES[r]] = p 
	return races_dict

### MAIN SCRAPE METHOD

def get_city_data(state, url):
	page = requests.get(url)
	soup = BeautifulSoup(page.text, 'html.parser')

	## get cities pop
	raw_pop_text = soup.find(class_='city-population').text
	city_pop = get_population(raw_pop_text)

	## get median age
	raw_median_text = soup.find(class_='median-age').text
	median_age = get_median_age(state, raw_median_text)

	## get proportion of races 
	raw_races_text = soup.find(class_='races-graph')
	prop_races_breakdown = get_prop_races_dict(raw_races_text)

	## get median gross rent
	raw_rent_text = soup.find(class_='median-rent').text
	median_gross_rent = get_median_gross_rent(raw_rent_text)

	final_dict = {}
	final_dict.update(city_pop)
	final_dict.update(median_age)
	final_dict.update(prop_races_breakdown)
	final_dict.update(median_gross_rent)
	return (state, final_dict)

def get_city_wrapper(state, url):
	try:
		return get_city_data(state, url)
	except Exception as e:
		write_to_err_log(state, url, e)
		return ERROR

### CSV METHODS 

def init_csv(path, name, fields):
	full_path = '{}/{}.csv'.format(path, name)
	with open(full_path, 'w+', newline='') as named_csv:
	    writer = csv.DictWriter(named_csv, fieldnames=fields)
	    writer.writeheader()

def init_state_csvs(dir_path):
	for state in ALL_STATES:
		init_csv(dir_path, state, STATE_CSV_COLNS)

# pass generator for rows
def write_to_csv(state, city_rows):
	with open('{}.csv'.format(state), 'w+', newline='') as state_csv:
		writer = csv.DictWriter(state_csv, fieldnames=STATE_CSV_COLNS)
		writer.writeheader()

		for city in city_rows:
			if city is ERROR:
				continue
			writer.writerow(city)

### ERROR HANDLING METHODS

def init_err_log(dir_path):
	init_csv(dir_path, 'error_log', ERR_CSV_COLNS)

def write_to_err_log(state, url, err):
	with open('error_log.csv', 'w+', newline='') as err_csv:
		writer = csv.DictWriter(state_csv, fieldnames=ERR_CSV_COLNS)
		writer.writerow({'state': state, 'url': url, 'error': err})

if __name__ == '__main__': 
	
	all_urls = generate_urls()
	path = '/Users/michaelusa/Desktop/city_data'
	#init_state_csvs(path)
	#init_err_log(path)

	for city_meta in all_urls:
		print(city_meta)
		#d = get_city_data(*city_meta)
		#break
	"""
	#d = get_city_data('Wisconsin', 'http://www.city-data.com/city/Hartford-Wisconsin.html')
	#print(d)
	#init_state_csvs()
	#init_err_log()


	







