# SumoSwissKnife

Your Sublime Text Swiss Knife For Sumo Logic.

Write your Sumo with smart completions, including all the metadata Sumo Logic to offer, execute Sumo Logic queries, format your queries' results in many different formats, with multi org connection manager.

![Preview](/Sumo_Sublime.gif)

## Installation
* Download the repository SumoSwissKnife-master.zip
* Unzip the contents to ~/Library/Application Support/Sublime Text 3/Packages/SumoSwissKnife
* Create a [Sumo Logic AccessID/AcessKey](https://help.sumologic.com/Manage/Security/Access-Keys#create-an-access-key) pair
* Start sublime, then from the command palette execute sumo: Setup Connection 
* Open/Create new, Sumo query file with .slql, select the newly created connection, and you are good to go

## Features

  Started it as a syntax highlight but then extended it to:
* Run queries
* Get all metadata (collectors, source categories, sources, users, roles, FERs, views and saved queries (personal folder))
* Output in major formats including but not limited to (CSV, Jira, Grid, Github, HTML etc.)
* Autocompletion including sumo metadata

## Give it a go!
* Create new file with ".sumo" extension
* Command Palette > Sumo: Setup Connections (Then add your AccessId/AccessKey)
* Select your connection
* try query:

``` ruby
| count by _sourceCategory

```
* Command Palette > Sumo: Run Current File Query
* Select time range
* Customize it if needed
* You may change the outpu format Command Palette > Sumo: Change Results Format
