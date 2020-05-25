import requests
import pandas as pd
import matplotlib.pyplot as plt

# Set the URL that contains at least one HTML table that you want to extract
url = 'https://www.animefillerlist.com/shows/one-piece'

# This line is used to "hack" the request to make the website think the request is coming from a web browser
# instead of a python script. You don't have to care why or how it works.
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
}

# Downloads all the HTML from the URL. All this HTML is now a very big "string" (text variable)
# which contains all the HTML tags. In these tags should be at least one `<table>...</table>` tags
text = requests.get(url, headers=headers).text

# Cool pandas function (`pd.read_html`) that will search for all `<table>` tags. It will save 
# all the table data in a list of Pandas dataframes (Series and Dataframes are the two main objects in the Pandas library,
# Dataframes are like Excel spreadsheets containing rows, and columns.
# Series = one dimensional array; DataFrame = two dimensional array)
# If the site contains one table, you just extract the first table ([0]), if the site contains 3 tables,
# and you only care about the 2nd table, you extract the second item in the list ([1]) (First item in a list starts counting at 0)
df = pd.read_html(text)[0]

# Now that I have a dataframe object (in the variable `df`) I perform Pandas operations such as aggregating and sorting functions.
one_piece = df.groupby('Type').size().sort_values()

# Create a pie chart
one_piece.plot.pie(autopct='%1.1f%%')
plt.title('One Piece Filler')
plt.show()
