
# IMPORTS 
import requests
from bs4 import BeautifulSoup

BASE_URL = 'http://www.city-data.com/city/{}-{}.html'
ALL_STATES = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Loiusiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New-Hampshire', 'New-Jersey', 'New-Mexico', 'New-York', 'North-Carolina', 'North-Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode-Island', 'South-Carolina', 'South-Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West-Virginia', 'Wisconsin', 'Wyoming']
ALL_FILES = ['alabama.txt', 'alaska.txt', 'arizona.txt', 'arkansas.txt', 'california.txt', 'colorado.txt', 'connecticut.txt', 'delaware.txt', 'florida.txt', 'georgia.txt', 'hawaii.txt', 'idaho.txt', 'illinois.txt', 'indiana.txt', 'iowa.txt', 'kansas.txt', 'kentucky.txt', 'loiusiana.txt', 'maine.txt', 'maryland.txt', 'massachusetts.txt', 'michigan.txt', 'minnesota.txt', 'mississippi.txt', 'missouri.txt', 'montana.txt', 'nebraska.txt', 'nevada.txt', 'new_hampshire.txt', 'new_jersey.txt', 'new_mexico.txt', 'new_york.txt', 'north_carolina.txt', 'north_dakota.txt', 'ohio.txt', 'oklahoma.txt', 'oregon.txt', 'pennsylvania.txt', 'rhode_island.txt', 'south_carolina.txt', 'south_dakota.txt', 'tennessee.txt', 'texas.txt', 'utah.txt', 'vermont.txt', 'virginia.txt', 'washington.txt', 'west_virginia.txt', 'wisconsin.txt', 'wyoming.txt']
RACES = {'White alone': 'White', 'Hispanic':'Hispanic', 'Asian alone':'Asian', 'Two or more races':'Mixed', 'American Indian alone': 'Native American', 'Black alone':'Black', 'Native Hawaiian and OtherPacific Islander alone': 'Native Hawaiian/Pacific Islander', 'Other race alone':'Other'}
LEGAL_STATES = ['Alaska', 'Oregon', 'Washington', 'California', 'Nevada', 'Colorado', 'Massachusetts', 'Maine', 'Vermont']

def generate_urls():
	url_list = []
	for f, s in zip(ALL_FILES, ALL_STATES):
		state_path = 'cities/{}'.format(f)
		with open(state_path, 'r') as state_cities:
			for city in state_cities.readlines():
				url_list.append(format_url(s, city))
	return (state_url for state_url in url_list)

def format_url(state, city):
	form_city = '-'.join(city.split(' '))
	return (state, BASE_URL.format(form_city, state))

### SCRAPED SITE PARSE METHODS

def get_population(soup):
	pop = soup.split(' ')
	pop = int((pop[len(pop)-1]).replace(',', ''))
	return {'population': pop}

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
	return {'race_proportions': races_dict}

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
	return final_dict

if __name__ == '__main__':
	"""
	for s in ALL_FILES:
		s_path = 'cities/{}'.format(s)
		with open(s_path, 'r+') as state_cities:
			all_lines = state_cities.readlines()
			wr = open(s_path, 'w')
			for l in all_lines:
				split_town = l.split(' ')
				new_town = []
				for word in split_town:
					new_town.append(word.capitalize())
				new_town = ' '.join(new_town)
				wr.write(new_town)
	"""

	d = get_city_data('Illinois', 'http://www.city-data.com/city/Chicago-Illinois.html')
	print(d)

	







