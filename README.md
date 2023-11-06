# multinational-retail-data-centralisation

## Tracking my progress.

6th Nov: data cleaning

- I started by importing the user_data variable from my data_extraction module - this contains the pandas dataframe to be cleaned.
- I examined the table.
- I made a copy of the table so that I could test my fixes without altering the original dataframe. Once I knew how to fix it I would code it in the class method.
- I set the index column to the already named "index" column
- I checked the the number of unique index values matched the number of rows in the table using `ud_copy.index.nunique()`
- I examined the datatypes in the database - these were all objects.
- First I converted the "first_name" and "last_name" columns to "string" datatypes.
- Then I went about converting the DOB column to a datetime value. This was trickier because - when I looked at the head of the dataframe, the date values seemed to be uniformatlly presented, but I soon discovered that the dates were present in many different formats. And there were also non-date-type values too.
- I had first used a mask to extract the DOB values that wouldn't be parsable by a simple `pd.to_datetime()` method with the format argument `format='%Y-%m-%d'`. Here I realised that there were many other date-formats to parse, as well as non-date values:
```
mask = pd.to_datetime(ud_copy.date_of_birth, format='%Y-%m-%d', errors='coerce').notna()
inverted_mask = np.invert(mask)
ud_copy.date_of_birth[inverted_mask].unique()
```
This showed me certain values:
!()[readme-images/code-snippet-1.png]

- I experimented with `dateutil.parser` `parse` method: `ud_copy['date_of_birth'] = ud_copy['date_of_birth'].apply(parse)` But because of the non-datetype values, the method was throwing a ParserError at the first non-date value, and I wasnt able to find a way around this.
- I eventually circled back to the `pd.to_datetime()` method, passing it the format argument `'mixed`: `ud_copy.date_of_birth = pd.to_datetime(ud_copy.date_of_birth, format='mixed', errors='coerce')`
- I checked the erroneous valids would be successfully deleted using this method:

```
mask2 = pd.to_datetime(ud_copy.date_of_birth, format='mixed', errors='coerce').notna()
inverted_mask2 = np.invert(mask2)
ud_copy.date_of_birth[inverted_mask2].unique()
```

- I then viewed the dateframe in `sort_value` mode according to `date_of_birth` (`ud_copy.sort_values(by='date_of_birth', axis=0).tail(50)`)and noticed that there were other columns that had the same mixed letter and number string types that I had seen polluting the DOB column.
- I went back to the "first_name" column, and used a regex expression to match all the rows that had a numerical digit in the first name:

`ud_copy.loc[ud_copy["first_name"].str.match(".*[0-9]+.*", "first_name")]`

 these rows had uniformally bogus data across the columns, so I deleted all of them.

 > Note to self: If I deleted these rows first, I may be able to use the dateutil.parser parse method on the DOB to datetime casting. Check if it is more reliable than pandas built-in method with `format='mixed'`.

- When I called the `.describe()` method on the DOB column after making these fixes, I noticed an age range from 1938 (which was plausible, making this person 85 years old) to 2006, which I believed to be potentially plausible but dubious - this person would be 16, almost 17, at the time of writing, so it might be a value worth querying with the data provider.