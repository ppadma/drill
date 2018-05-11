/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill;

import ch.qos.logback.classic.Level;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassBuilder;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.DrillTest;
import org.junit.Rule;
import org.junit.Test;

public class DRILL4897 extends DrillTest {

    public static final String GENERATED_SOURCES_DIR = "/Users/karthik/drill/generated-code/";
    @Rule
    public final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

    @Test
    public void testMD4897() throws Exception {

        LogFixture.LogFixtureBuilder logBuilder = LogFixture.builder()
                .logger("org.apache.drill", Level.TRACE).toConsole();

        try (//LogFixture logs = logBuilder.build();
             ClusterFixture cluster = ClusterFixture.builder(dirTestWatcher)
                     .configProperty(ClassBuilder.CODE_DIR_OPTION, GENERATED_SOURCES_DIR)
                     .configProperty(ExecConstants.BIT_RPC_TIMEOUT, 0)
                     .configProperty(ExecConstants.USER_RPC_TIMEOUT, 0)
                     .configProperty(ExecConstants.BIT_TIMEOUT, 0)
                     .build();


             ClientFixture client = cluster.clientFixture()) {
            cluster.defineWorkspace("dfs", "data", "/Users/karthik/work/bugs/DRILL-4897", "csv");
//            String sql = "select CAST(case isnumeric(columns[0]) WHEN 0 THEN 2147483647 ELSE columns[0] END AS BIGINT) from `dfs.data`.`pw2.csv` ";
//            String sql = "select * from cp.`store/json/input2.json`";
//            String sql = "select * from cp.`employee.json`";

//            String sql= "select convert_to(rl[1], 'JSON') list_col from cp.`store/json/input2.json`";
//            String sql= "select convert_from(convert_to(rl[1], 'JSON'), 'JSON') list_col from cp.`store/json/input2.json`";
//            String sql = "select employee_id as eid " +
//                    "                          , employee_id + position_id as eidpluspid " +
//                    "                   from cp.`employee.json` ";
//            employee_id<BIGINT(OPTIONAL)>,full_name<VARCHAR(OPTIONAL)>,first_name<VARCHAR(OPTIONAL)>,last_name<VARCHAR(OPTIONAL)>,position_id<BIGINT(OPTIONAL)>,position_title<VARCHAR(OPTIONAL)>,store_id<BIGINT(OPTIONAL)>,department_id<BIGINT(OPTIONAL)>,birth_date<VARCHAR(OPTIONAL)>,hire_date<VARCHAR(OPTIONAL)>,salary<FLOAT8(OPTIONAL)>,supervisor_id<BIGINT(OPTIONAL)>,education_level<VARCHAR(OPTIONAL)>,marital_status<VARCHAR(OPTIONAL)>,gender<VARCHAR(OPTIONAL)>,management_role<VARCHAR(OPTIONAL)>
//            1,Sheri Nowmer,Sheri,Nowmer,1,President,0,1,1961-08-26,1994-12-01 00:00:00.0,80000.0,0,Graduate Degree,S,F,Senior Management
//            String sql = "SELECT MAX(employee_id) as max_id " +
//                         " from cp.`employee.json` ";
            String sql = "SELECT UPPER(CONCAT(first_name, '##')) as upper_name " +
                    " from cp.`employee.json` ";

//            String sql = "SELECT employee_id + position_id as eidpluspid " +
//                            " from cp.`employee.json` ";
            //String sql = "select  from cp.`employee.json` limit 1 ";
//            String sql = "select employee_id + position_id as eidpluspid from cp.`employee.json` ";
            //String sql = "select employee_id as eid, employee_id as eid2, employee_id + position_id as eidpluspid from cp.`employee.json` ";

            client.queryBuilder().sql(sql).printCsv();
        }
    }
}