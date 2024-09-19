"""
Template Component main class.

"""
import csv
import logging
from typing import List, Dict

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement, ValidationResult

import keboolaApi.client as client

# configuration variables
KEY_OUTPUT_LIST_FLOWS = 'output_list_flows'
KEY_FLOW_TRIGGER_IDS = 'flow_trigger_ids'


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self.client = None

    def run(self):
        """
        Main execution code
        """
        self._init_configuration()
        params = self.configuration.parameters

        # reset triggers
        if params.get(KEY_FLOW_TRIGGER_IDS) or len(params.get(KEY_FLOW_TRIGGER_IDS)) > 0:
            triggers = self._list_triggers(params.get(KEY_FLOW_TRIGGER_IDS))
            for trigger in triggers:
                # Remove triggers
                if trigger:
                    logging.info(
                        f"Reset trigger id:{trigger.get('id')} "
                        f"for flow name:{trigger.get('configuration_detail').get('name')}")
                    new_trigger_conf = self._prep_new_trigger_configuration(trigger)
                    self.client.create_trigger(new_trigger_conf)
                    self.client.remove_trigger(trigger.get('id'))

        # List triggers to output
        if params.get(KEY_OUTPUT_LIST_FLOWS):
            # List triggers
            triggers = self._list_triggers()
            if triggers:
                columns = ['flow_configuration_id',
                           'trigger_last_run',
                           'flow_configuration_name',
                           'selected_table_id',
                           'selected_table_is_expected',
                           'selected_table_last_import_date']

                # Create output table (Tabledefinition - just metadata)
                out_table = self.create_out_table_definition('flows_with_trigger.csv',
                                                             primary_key=['flow_configuration_id', 'selected_table_id'])
                logging.info(out_table.full_path)

                # Create output table (Tabledefinition - just metadata)
                with open(out_table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
                    # write result with column added
                    writer = csv.DictWriter(out_file, fieldnames=columns, dialect='kbc')
                    writer.writeheader()

                    for trigger in triggers:
                        for table in trigger.get('tables'):
                            writer.writerow({'flow_configuration_id': trigger.get('configuration_detail').get('id'),
                                             'trigger_last_run': trigger.get('lastRun'),
                                             'flow_configuration_name': trigger.get('configuration_detail').get('name'),
                                             'selected_table_id': table.get('tableId'),
                                             'selected_table_is_expected': table.get('table_detail').get('is_expected',
                                                                                                         None),
                                             'selected_table_last_import_date': table.get('table_detail').get(
                                                 'lastImportDate',
                                                 None)})

                # Save table manifest (output.csv.manifest) from the tabledefinition
                self.write_manifest(out_table)

    def _check_environments_variables(self):
        """
        Check the presence of required environment variables and log an error if any are missing.
        """
        if not self.environment_variables.token:
            logging.error("Environment variable self.environment_variables.token not found!")

        if not self.environment_variables.url:
            logging.error("Environment variable url not found!")

    def _init_configuration(self):
        self._check_environments_variables()
        # Access parameters in data/config.json
        self.client = client.KeboolaClient(self.environment_variables.token, self.environment_variables.url)

    def _list_triggers(self, flow_ids=None):
        """
        Get list of triggers from the client
        """
        if flow_ids:
            triggers: List[Dict] = self.client.get_trigger(flow_ids)
        else:
            triggers = self.client.get_triggers()

        for trigger in triggers:
            # Add configuration details to the trigger
            try:
                configuration_detail = self.client.get_component_configuration_detail(trigger.get('component'),
                                                                                      trigger.get('configurationId'))
                trigger['configuration_detail'] = configuration_detail
            except client.KeboolaClientException as e:
                logging.debug(
                    f"Error while get_configuration_detail "
                    f"for {trigger.get('component')}:{trigger.get('configurationId')} "
                    f"for trigger {trigger.get('id')}: {e}")

            # Add table details to the trigger
            for table in trigger.get('tables'):
                table_detail = self.client.get_table_detail(table.get('tableId'))
                if table_detail:
                    table_detail['is_expected'] = self._is_expected(trigger.get('lastRun'),
                                                                    table_detail.get('lastImportDate'))
                    table['table_detail'] = table_detail
                # add some flag if some tables are missing
                else:
                    trigger['some_tables_missing'] = True
        return [trigger for trigger in triggers if trigger.get('configuration_detail', None)]

    @sync_action('list_flows')
    def list_flows(self):
        """
        List all flows and formate it to a list of SelectElement
        """
        self._init_configuration()
        return [SelectElement(label=trigger.get('configuration_detail').get('name'),
                              value=trigger.get('configuration_detail').get('id')) for trigger in self._list_triggers()]

    @sync_action('flow_detail')
    def flow_detail(self):
        """
        List all flows and format it to a Markdown table
        """
        self._init_configuration()
        params = self.configuration.parameters
        # Get detail of triggers
        if params.get(KEY_FLOW_TRIGGER_IDS):
            # Initialize the Markdown table
            markdown_table = "| Flow | Trigger Last Run | Selected Tables | Last Import | Is expected |\n"
            markdown_table += "|------|------------------|-----------------|-------------|-------------|\n"
            # Fill in the table rows
            markdown_table += ''.join(
                f"| **{trigger['configuration_detail']['name']}** "
                f"| {trigger['lastRun']} "
                f"| **{table['table_detail']['id']}** "
                f"| {table['table_detail']['lastImportDate']}) "
                f"| {table['table_detail']['is_expected']} |\n"
                for trigger in self._list_triggers(params.get(KEY_FLOW_TRIGGER_IDS))
                for table in trigger.get('tables')
            )

            # Return the Markdown table
            return ValidationResult(message=markdown_table)

    @staticmethod
    def _prep_new_trigger_configuration(trigger):
        new_trigger_conf = {
            'runWithTokenId': trigger.get('runWithTokenId'),
            'component': trigger.get('component'),
            'configurationId': trigger.get('configurationId'),
            'coolDownPeriodMinutes': trigger.get('coolDownPeriodMinutes'),
            'tableIds': [tbl.get('tableId') for tbl in trigger.get('tables')]
        }
        return new_trigger_conf

    @staticmethod
    def _is_expected(last_run, last_import):
        if last_run >= last_import:
            return True
        else:
            return False


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
