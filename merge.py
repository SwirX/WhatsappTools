import os
from os.path import isfile, abspath, exists, isdir, samefile
from sys import argv
import sqlite3


def create_database(olddb, name: str = "merged") -> [bool, str]:
    _status = False
    try:
        with open(f'{abspath(olddb)}', 'rb') as old_db:
            db_bytes = old_db.read()
            with open("merged.db", "wb+") as merged_db:
                merged_db.write(db_bytes)
        _status = True
    except Exception as exception:
        print("an error has occurred")
        print(exception)

    return _status, abspath("merged.db")



def create_insert_command(table_name, entries_count):
    print(f"INSERT INTO {table_name} VALUES (" + ("?, " * (entries_count - 1)) + "?);")
    return f"INSERT INTO {table_name} VALUES (" + ("?, " * (entries_count - 1)) + "?);"


def validate_arguments(first_file, second_file):
    if not isfile(first_file) and not isfile(second_file):
        print("the passed paths are not files")
        exit(1)
    # check if the passed files are the same file or not
    if samefile(first_file, second_file):
        print("the passed files are the same")
        exit(1)
    # check if the passed files are a database file (.db)
    if not first_file.endswith(".db") and not second_file.endswith(".db"):
        if first_file.endswith(".crypt14") and second_file.endswith(".crypt14"):
            print("Still not implemented")
            exit(1)
        print("the passed files are not a database file (ends with .db)\nRecheck the files and retry")
        exit(1)


try:
    old_database_file = argv[1].replace("\\", "/")
    new_database_file = argv[2].replace("\\", "/")
    validate_arguments(old_database_file, new_database_file)

    outputDir = "./output"
    try:
        if argv[3] is not None:
            if isdir(argv[3]) and exists(argv[3]):
                outputDir = argv[3]
    except IndexError:
        pass
    # check if the directory exists if not create one
    if not exists(outputDir):
        os.system(f"mkdir {outputDir}")

    old_database_connection = sqlite3.connect(old_database_file)
    new_database_connection = sqlite3.connect(new_database_file)
    old_database_cursor = old_database_connection.cursor()
    new_database_cursor = new_database_connection.cursor()

    l = [a for a in old_database_cursor.execute
    ("SELECT name FROM sqlite_master WHERE type = 'trigger'")]

    # these values have been hardcoded here so that we don't unnecessarily recompute them (might change it later)
    tables = {'props', 'conversion_tuples', 'labeled_messages_fts_content',
              'labeled_messages_fts_segments', 'labeled_messages_fts_segdir', 'message_vcard_jid',
              'receipt_orphaned', 'message_add_on_receipt_device', 'message_quoted_group_invite_legacy',
              'message', 'group_participant_user', 'message_system_value_change', 'newsletter', 'jid',
              'message_quoted_media', 'payment_background_order', 'primary_device_version',
              'message_ui_elements_reply', 'message_external_ad_content', 'away_messages', 'jid_map',
              'message_quoted_mentions', 'message_quoted_blank_reply', 'message_system',
              'message_system_business_state', 'messages_quotes', 'message_system_linked_group_call',
              'status_list', 'quick_replies', 'message_payment_status_update', 'group_notification_version',
              'mms_thumbnail_metadata', 'audio_data', 'joinable_call_log', 'message_ephemeral',
              'message_system_payment_invite_setup', 'media_refs', 'message_system_device_change',
              'integrator_display_name', 'message_view_once_media', 'scheduled_calls', 'labels',
              'message_quoted_product', 'messages_hydrated_four_row_template', 'message_text', 'priority_inbox',
              'quick_reply_attachments', 'message_quoted_payment_invite', 'message_details',
              'message_add_on_keep_in_chat', 'status', 'message_scheduled_call',
              'message_system_with_group_nodes', 'group_participant_device',
              'message_system_initial_privacy_provider', 'pay_transaction', 'group_past_participant_user',
              'receipt_user', 'message_add_on_pin_in_chat', 'message_add_on_reaction',
              'parent_group_participants', 'message_quoted_order', 'message_broadcast_ephemeral',
              'message_thumbnail', 'message_product', 'message_ephemeral_sync_response',
              'message_system_scheduled_call_start', 'message_thumbnails', 'message_poll_option',
              'payment_background', 'call_log', 'message_forwarded', 'smart_suggestions_key_value',
              'message_quoted_text', 'message_system_sibling_group_link_change', 'message_quoted',
              'message_system_number_change', 'message_link', 'message_privacy_state',
              'message_system_ephemeral_setting_not_applied', 'receipt_device',
              'message_media_interactive_annotation_vertex', 'message_order',
              'message_media_interactive_annotation', 'chat', 'message_system_block_contact',
              'message_group_invite', 'message_ui_elements', 'keywords', 'labeled_messages', 'user_device_info',
              'group_participants_history', 'message_revoked', 'message_vcard', 'message_streaming_sidecar',
              'message_quoted_ui_elements', 'message_add_on_poll_vote', 'message_quoted_ui_elements_reply_legacy',
              'call_link', 'message_secret', 'message_poll', 'message_quoted_location', 'played_self_receipt',
              'message_system_photo_change', 'message_template_quoted', 'message_template', 'labeled_jids',
              'newsletter_linked_account', 'missed_call_logs', 'suggest_as_you_type', 'lid_display_name',
              'message_system_chat_assignment', 'message_send_count', 'message_orphaned_edit', 'lid_chat_state',
              'backup_changes', 'frequents', 'status_crossposting', 'message_rating', 'message_payment',
              'message_add_on_poll_vote_selected_option', 'message_ephemeral_setting', 'message_system_group',
              'message_quoted_ui_elements_reply', 'message_template_button', 'message_location',
              'message_status_psa_campaign', 'frequent', 'message_add_on_orphan', 'media_hash_thumbnail',
              'agent_message_attribution', 'template_messages_metadata', 'community_chat',
              'missed_call_log_participant', 'message_media_vcard_count', 'agent_devices', 'quick_reply_usage',
              'message_quoted_vcard', 'labeled_jid', 'message_invoice', 'message_mentions',
              'quick_reply_keywords', 'message_system_group_with_parent',
              'message_payment_transaction_reminder', 'newsletter_message', 'invoice_transactions',
              'message_edit_info', 'group_participants', 'message_quote_invoice', 'agent_chat_assignment',
              'message_media', 'user_device', 'receipts', 'deleted_chat_job', 'suggested_replies',
              'message_system_chat_participant', 'message_system_community_link_changed',
              'message_payment_invite', 'call_log_participant_v2', 'message_add_on', 'message_future',
              'message_quoted_group_invite', 'android_metadata', 'labeled_messages_fts'}
    views = {'message_view', 'available_message_view', 'deleted_messages_view', 'deleted_messages_ids_view',
             'chat_view'}
    triggers = {'call_log_bd_for_call_log_participant_v2_trigger', 'call_log_bd_for_joinable_call_log_trigger',
                'chat_bd_for_community_chat_trigger', 'chat_bd_for_message_add_on_orphan_trigger',
                'chat_bd_for_message_link_trigger', 'chat_bd_for_newsletter_linked_account_trigger',
                'chat_bd_for_newsletter_trigger', 'group_participant_user_bd_for_group_participant_device_trigger',
                'labels_bd_for_labeled_jid_trigger', 'labels_bd_for_labeled_jids_trigger',
                'labels_bd_for_labeled_messages_trigger', 'message_add_on_bd_for_message_add_on_keep_in_chat_trigger',
                'message_add_on_bd_for_message_add_on_pin_in_chat_trigger',
                'message_add_on_bd_for_message_add_on_poll_vote_selected_option_trigger',
                'message_add_on_bd_for_message_add_on_poll_vote_trigger',
                'message_add_on_bd_for_message_add_on_reaction_trigger',
                'message_add_on_bd_for_message_add_on_receipt_device_trigger',
                'message_bd_for_agent_message_attribution_trigger', 'message_bd_for_audio_data_trigger',
                'message_bd_for_labeled_messages_fts_trigger', 'message_bd_for_labeled_messages_trigger',
                'message_bd_for_message_add_on_trigger', 'message_bd_for_message_broadcast_ephemeral_trigger',
                'message_bd_for_message_details_trigger', 'message_bd_for_message_edit_info_trigger',
                'message_bd_for_message_ephemeral_setting_trigger', 'message_bd_for_message_ephemeral_trigger',
                'message_bd_for_message_external_ad_content_trigger', 'message_bd_for_message_forwarded_trigger',
                'message_bd_for_message_ftsv2_trigger', 'message_bd_for_message_future_trigger',
                'message_bd_for_message_group_invite_trigger', 'message_bd_for_message_link_trigger',
                'message_bd_for_message_location_trigger', 'message_bd_for_message_media_trigger',
                'message_bd_for_message_mentions_trigger', 'message_bd_for_message_order_trigger',
                'message_bd_for_message_payment_invite_trigger', 'message_bd_for_message_poll_trigger',
                'message_bd_for_message_privacy_state_trigger', 'message_bd_for_message_product_trigger',
                'message_bd_for_message_quoted_trigger', 'message_bd_for_message_rating_trigger',
                'message_bd_for_message_revoked_trigger', 'message_bd_for_message_scheduled_call_trigger',
                'message_bd_for_message_secret_trigger', 'message_bd_for_message_send_count_trigger',
                'message_bd_for_message_status_psa_campaign_trigger',
                'message_bd_for_message_streaming_sidecar_trigger', 'message_bd_for_message_system_trigger',
                'message_bd_for_message_template_trigger', 'message_bd_for_message_text_trigger',
                'message_bd_for_message_thumbnail_trigger', 'message_bd_for_message_ui_elements_reply_trigger',
                'message_bd_for_message_ui_elements_trigger', 'message_bd_for_message_vcard_jid_trigger',
                'message_bd_for_message_vcard_trigger', 'message_bd_for_message_view_once_media_trigger',
                'message_bd_for_messages_hydrated_four_row_template_trigger', 'message_bd_for_missed_call_logs_trigger',
                'message_bd_for_mms_thumbnail_metadata_trigger', 'message_bd_for_newsletter_message_trigger',
                'message_bd_for_played_self_receipt_trigger', 'message_bd_for_receipt_device_trigger',
                'message_bd_for_receipt_user_trigger', 'message_bd_for_status_crossposting_trigger',
                'message_bd_for_suggest_as_you_type_trigger', 'message_bd_for_suggested_replies_trigger',
                'message_media_bd_for_message_media_interactive_annotation_trigger',
                'message_media_bd_for_message_media_vcard_count_trigger',
                'message_media_interactive_annotation_bd_for_message_media_interactive_annotation_vertex_trigger',
                'message_poll_bd_for_message_poll_option_trigger',
                'message_quoted_bd_for_message_quoted_blank_reply_trigger',
                'message_quoted_bd_for_message_quoted_group_invite_trigger',
                'message_quoted_bd_for_message_quoted_location_trigger',
                'message_quoted_bd_for_message_quoted_media_trigger',
                'message_quoted_bd_for_message_quoted_mentions_trigger',
                'message_quoted_bd_for_message_quoted_order_trigger',
                'message_quoted_bd_for_message_quoted_payment_invite_trigger',
                'message_quoted_bd_for_message_quoted_product_trigger',
                'message_quoted_bd_for_message_quoted_text_trigger',
                'message_quoted_bd_for_message_quoted_ui_elements_reply_trigger',
                'message_quoted_bd_for_message_quoted_ui_elements_trigger',
                'message_quoted_bd_for_message_quoted_vcard_trigger',
                'message_quoted_bd_for_message_template_quoted_trigger',
                'message_system_bd_for_message_payment_status_update_trigger',
                'message_system_bd_for_message_payment_transaction_reminder_trigger',
                'message_system_bd_for_message_payment_trigger',
                'message_system_bd_for_message_system_block_contact_trigger',
                'message_system_bd_for_message_system_business_state_trigger',
                'message_system_bd_for_message_system_chat_assignment_trigger',
                'message_system_bd_for_message_system_chat_participant_trigger',
                'message_system_bd_for_message_system_community_link_changed_trigger',
                'message_system_bd_for_message_system_device_change_trigger',
                'message_system_bd_for_message_system_ephemeral_setting_not_applied_trigger',
                'message_system_bd_for_message_system_group_trigger',
                'message_system_bd_for_message_system_group_with_parent_trigger',
                'message_system_bd_for_message_system_initial_privacy_provider_trigger',
                'message_system_bd_for_message_system_linked_group_call_trigger',
                'message_system_bd_for_message_system_number_change_trigger',
                'message_system_bd_for_message_system_payment_invite_setup_trigger',
                'message_system_bd_for_message_system_photo_change_trigger',
                'message_system_bd_for_message_system_scheduled_call_start_trigger',
                'message_system_bd_for_message_system_sibling_group_link_change_trigger',
                'message_system_bd_for_message_system_value_change_trigger',
                'message_system_bd_for_message_system_with_group_nodes_trigger',
                'message_template_bd_for_message_template_button_trigger',
                'message_vcard_bd_for_message_vcard_jid_trigger',
                'missed_call_logs_bd_for_missed_call_log_participant_trigger',
                'payment_background_bd_for_payment_background_order_trigger',
                'quick_replies_bd_for_quick_reply_attachments_trigger',
                'quick_replies_bd_for_quick_reply_keywords_trigger', 'quick_replies_bd_for_quick_reply_usage_trigger',
                'suggest_as_you_type_delete_oldest_trigger', 'suggested_replies_delete_oldest_trigger'}

    added_tables = []
    added_views = []
    added_triggers = []

    status, path = create_database(old_database_file)
    if not status:
        print("an error has occurred while creating the merged file")
        exit(1)
    merged_database_connection = sqlite3.connect(path)
    merged_database_cursor = merged_database_connection.cursor()

    for table in tables:
        if table in added_tables:
            continue
        skip_merging = table == "android_metadata"
        print("\n" + table + "\n")
        old_db_table_entries = [a for a in old_database_cursor.execute(f"SELECT * FROM {table}")]
        new_db_table_entries = [a for a in new_database_cursor.execute(f"SELECT * FROM {table}")]
        sql_create_table_function = old_database_cursor.execute(f"SELECT sql FROM sqlite_master WHERE name = '{table}' "
                                                                "AND type = 'table'").fetchone()[0].replace("FTS3(", "FTS4(")
        print("create function: ", sql_create_table_function)
        merged_database_cursor.execute(sql_create_table_function)
        last_id = int(
            old_database_connection.execute("SELECT * FROM message ORDER BY _id DESC LIMIT 1").fetchone()[0]) or 0
        if last_id == 0:
            continue

        merged_entries = []

        for e in old_db_table_entries:
            merged_entries.append(e)

        if not skip_merging:
            for e in new_db_table_entries:
                if not e or e is None:
                    break
                if e in merged_entries:
                    print("merged. skipping...")
                    continue
                params = ()
                try:
                    new_id = last_id + e[0]
                    params += (new_id,)
                    if type(e[1]) == int or type(e[1]) == tuple:
                        try:
                            params += (new_id,)
                            for i in range(2, len(e)):
                                params += (e[i],)
                        except Exception as ex:
                            print(ex)
                    elif type(e[1]) == str:
                        continue
                    else:
                        for i in range(1, len(e)):
                            params += (e[i],)
                except Exception as exp:
                    print(exp)
                print(params)
                merged_database_cursor.execute(create_insert_command(table, len(e)), params)
            added_tables.append(table)

        merged_database_connection.commit()

    for view in views:
        if view in added_views:
            continue
        print("\n" + view + "\n")
        sql_create_view_function = new_database_cursor.execute(
            f"SELECT sql FROM sqlite_master WHERE name = '{view}' AND type = 'view'").fetchone()[0]
        merged_database_cursor.execute(sql_create_view_function)
        added_views.append(view)
        merged_database_connection.commit()

    for trigger in triggers:
        if trigger in added_triggers:
            continue
        print("\n" + trigger + "\n")
        sql_create_trigger_function = new_database_cursor.execute(
            f"SELECT sql FROM sqlite_master WHERE name = '{trigger}' AND type = 'trigger'").fetchone()[0]
        merged_database_cursor.execute(sql_create_trigger_function)
        added_triggers.append(trigger)
        merged_database_connection.commit()

    print("merged the data")
    print("saving the file")
    merged_database_connection.close()
    old_database_connection.close()
    new_database_connection.close()


except IndexError as e:
    print(e)
    exit(1)
