# frankenbot
A tool for Springbot data extraction, transformation, and insertion

You'll need a golang environment on your machine to run the frankenbot. I recommend setting it up with [asdf](https://github.com/asdf-vm/asdf]).

Once you have that, all you need to do is:

```
git clone git@github.com:serracorwin/frankenbot.git
cd frankenbot
go run *.go /path/to/config.yml
```

## Configuration
The example.yml shows all the required and optional configuration options. All but the filters and since options are required. The way filters work is you first list the table name, then column and replacement pairs. The way since works is you list the table name, then the date field and how far back you want to go.
