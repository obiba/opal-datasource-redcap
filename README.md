# Opal REDCap Plugin [![Build Status](https://travis-ci.org/obiba/opal-datasource-redcap.svg?branch=master)](https://travis-ci.org/obiba/opal-datasource-redcap)

This plugin can import non-repeating longitudinal and non-longitudinal REDCap projects. For longitudinal projects. 

## How to use site.properties
This configuration file is meant to provide default values for REDCap datasource plugin. For example, to provide default values for a REDCap project URL and token, you can add add these in the `site.properties` file:

```properties
usage.IMPORT.url=http://test.org/redcap/api
usage.IMPORT.token=DKJSDLFJLFJEWLJEWAMDMASDMLWQEK
```
When Opal server starts and the plugin is loaded, these two values no longer need to be entered by the user in the UI, the above values will be used instead.

[Opal](https://github.com/obiba/opal) is OBiBa’s core database application for biobanks or epidemiological studies.

* Have a bug or a question? Please create an issue on [GitHub](https://github.com/obiba/opal-datasource-redcap/issues).
* Continuous integration is on [Travis](https://travis-ci.org/obiba/opal-datasource-redcap).

## Mailing list

Have a question? Ask on our mailing list!

obiba-users@googlegroups.com

[http://groups.google.com/group/obiba-users](http://groups.google.com/group/obiba-users)

## License

OBiBa software are open source and made available under the [GPL3 licence](http://www.obiba.org/pages/license/). OBiBa software are free of charge.

# OBiBa acknowledgments

If you are using OBiBa software, please cite our work in your code, websites, publications or reports.

"The work presented herein was made possible using the OBiBa suite (www.obiba.org), a  software suite developed by Maelstrom Research (www.maelstrom-research.org)"
