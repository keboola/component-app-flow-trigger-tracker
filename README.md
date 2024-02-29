app-flow-trigger-tracker
=============

Description

This component is used to reset the flow triggers in the Keboola platform.
Also can it used to get the flow triggers for a specific flow and their information about statuses.

Functionality notes
===================
Reset flow triggers = drop & create.

Configuration
=============

flow_trigger_ids
-------
List of flow configuration Ids to reset.

output_list_flows
-------
Hiden parameter for listing all flows in the project and write it to the output.

Output
======

flows_with_trigger

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/keboola/component-app-flow-trigger-tracker app-flow-trigger-tracker
cd app-flow-trigger-tracker
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
