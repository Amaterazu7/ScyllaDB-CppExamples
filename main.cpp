// Connecting to ScyllaDB with a simple C++ program
#include <cassandra.h>
#include <iostream>

int main(int argc, char* argv[]) {
    /**
     * START THE CONNECTION
     * */
    // Allocate the objects that represent cluster and session. Remember to free them once no longer needed!
    CassCluster* cluster = cass_cluster_new();
    CassSession* session = cass_session_new();
    CassFuture* connect_future;
    CassError rc;
    /**
     * You can specify more than one, comma-separated, but you don’t have to
     * - driver will discover other nodes by itself.
     * You should do it if you expect some of your contact points to be down.
     * */
    // Add the contact points. These can be either IPs or domain names.
    cass_cluster_set_contact_points(cluster, "localhost"); // set the IP according to your setup

    /**
     * Connect. `cass_session_connect` returns a pointer to "future"
     * Also, this allocates the object pointed to by `connect_future`,
     *   which must be freed manually (see below).
     * */
    connect_future = cass_session_connect(session, cluster);
    rc = cass_future_error_code(connect_future);
    // `cass_future_error_code` will block until connected or refused.
    if (rc == CASS_OK) {
        std::cout << "::: Connected :::: " << std::endl;

        /**
         * INSERT
         * */

        // Imagine we have a lot of mutants to INSERT and we don’t know their addresses nor do we have their pictures
        const char* insertQ = "INSERT INTO ks.mutant_data (first_name, last_name, address, picture_location) VALUES (?, ?, ?, ?);"; // Note the question marks

        CassFuture* prep_future = cass_session_prepare(session, insertQ); // Send the "templated" query to Scylla to "compile" it.
        cass_future_wait(prep_future); // Preparing (“compiling”) the query is an async operation.
        const CassPrepared* prepared = cass_future_get_prepared(prep_future); // This object will be reused for every similar INSERT
        cass_future_free(prep_future);

        // Now this code block can be repeated, effectively performing similar INSERTs - with various values in places of question marks
        {
            CassStatement* bound_statement = cass_prepared_bind(prepared);
            cass_statement_bind_string(bound_statement, 0, "Diego"); // Fill in the first question mark
            cass_statement_bind_string(bound_statement, 1, "Cañete"); // Fill in the second question mark
            cass_statement_bind_string(bound_statement, 2, "Castro Barros 868"); // Fill in the third question mark
            cass_statement_bind_string(bound_statement, 3, "http://www.facebook.com/dcanete"); // Fill in the fourth question mark

            // Proceed as usual:
            CassFuture* exec_future = cass_session_execute(session, bound_statement);
            cass_future_wait(exec_future);
            std::cout << "\n:: Inserting data using Prepared Statement :: \n" << std::endl;
            cass_future_free(exec_future);
            cass_statement_free(bound_statement);
        }

        cass_prepared_free(prepared);

        /**
        * FIN INSERT
        * */

        /**
         * SIMPLE SELECT
         * */
        // Fetch data sample from ScyllaDB after the connection is established
        const char* query = "SELECT first_name, last_name, address, picture_location FROM ks.mutant_data";
        CassStatement* statement = cass_statement_new(query, 0); // the 2nd argument (zero) is be explained in section “Prepared Statements”

        CassFuture* result_future = cass_session_execute(session, statement);

        if (cass_future_error_code(result_future) == CASS_OK) {
            const CassResult* result = cass_future_get_result(result_future);
            const CassRow* row = cass_result_first_row(result);

            if (row) {
                const CassValue* value = cass_row_get_column_by_name(row, "first_name");

                const char* first_name;
                size_t first_name_length;
                std::cout << "::: -- SIMPLE SELECT -- ::: \n";
                std::cout << "::: First name fetched is: ";
                cass_value_get_string(value, &first_name, &first_name_length);
                std::cout.write(first_name, first_name_length);
                std::cout << std::endl;

                /**
                 * ITERATION FROM RESULT
                 * */

                // Iterate all rows of `mutant_data` after `result` is fetched
                CassIterator* iterator = cass_iterator_from_result(result);
                std::cout << "::: -- ITERATOR -- :::";
                while (cass_iterator_next(iterator)) {
                    const CassRow* row = cass_iterator_get_row(iterator);

                    // Proceed with `row` as usual:
                    const CassValue* value1 = cass_row_get_column_by_name(row, "first_name");
                    const char* first_name;
                    size_t first_name_length;
                    cass_value_get_string(value1, &first_name, &first_name_length);
                    const CassValue* value2 = cass_row_get_column_by_name(row, "last_name");
                    const char* last_name;
                    size_t last_name_length;
                    cass_value_get_string(value2, &last_name, &last_name_length);

                    std::cout << "\n::: -- ::: \n";
                    std::cout.write(first_name, first_name_length);
                    std::cout << " ";
                    std::cout.write(last_name, last_name_length);
                }
                std::cout << "\n::: ------ ::: \n";
                cass_iterator_free(iterator);
                /**
                 * FIN ITERATION FROM RESULT
                 * */
            }
            cass_result_free(result);

            /**
             * SELECT WITH "WHERE" STATEMENT
             * */
            // Parameterized simple statement, not to be confused with prepared statements!
            CassStatement* statement = cass_statement_new("SELECT * FROM ks.mutant_data WHERE first_name=? and last_name=?", 2); // `2` is the number of parameters
            cass_statement_bind_string(statement, 0, "Bob");
            cass_statement_bind_string(statement, 1, "Loblaw");
            // Proceed with `statement` as usual
            /**
             * FIN SELECT WITH "WHERE" STATEMENT
             * */

            /**
             * DELETE STATEMENT
             * */
            const char* deleteQ = "DELETE FROM ks.mutant_data WHERE first_name = ? AND last_name = ? ;"; // Note the question marks

            CassFuture* prep_delete_future = cass_session_prepare(session, deleteQ); // Send the "templated" query to Scylla to "compile" it.
            cass_future_wait(prep_delete_future); // Preparing (“compiling”) the query is an async operation.
            const CassPrepared* prepared_delete = cass_future_get_prepared(prep_delete_future); // This object will be reused for every similar INSERT
            cass_future_free(prep_delete_future);

            // Now this code block can be repeated, effectively performing similar INSERTs - with various values in places of question marks
            {
                CassStatement* delete_statement = cass_prepared_bind(prepared_delete);
                cass_statement_bind_string(delete_statement, 0, "Diego"); // Fill in the first question mark
                cass_statement_bind_string(delete_statement, 1, "Cañete"); // Fill in the second question mark

                // Proceed as usual:
                CassFuture* exec_future_delete = cass_session_execute(session, delete_statement);
                cass_future_wait(exec_future_delete);
                std::cout << "\n:: Removing data using Prepared Statement :: \n" << std::endl;
                cass_future_free(exec_future_delete);
                cass_statement_free(delete_statement);
            }

            cass_prepared_free(prepared_delete);
            /**
             * FIN DELETE STATEMENT
             * */

        } else {
            // Handle error - omitted for brevity
            std::cout << "::: ------ ::: \n";
            std::cout << "::: ERROR FUTURE ::: \n";
        }

        cass_statement_free(statement);
        cass_future_free(result_future);

        /**
         * FIN SIMPLE SELECT
         * */
        /**
         * CREATE INDEX
                const char* idxQuery = "CREATE INDEX idx on ks.mutant_data (picture_location) ;";
                // Proceed as usual:
                CassStatement* idxStatement = cass_statement_new(idxQuery, 0);
                CassFuture* future = cass_session_execute(session, idxStatement);
                cass_future_wait(future);
         * FIN CREATE INDEX
         **/

    } else {
        const char* message;
        size_t message_l;
        cass_future_error_message(connect_future, &message, &message_l);
        std::cout << "::: Connection ERROR ::: \n" << std::endl;
        std::cout << ":::  " << message << " ::: \n"<< std::endl;

    }

    // Release the resources.
    cass_future_free(connect_future);
    cass_cluster_free(cluster);
    cass_session_free(session);
    /**
     * RELEASE CONNECTION ?
     * */
}