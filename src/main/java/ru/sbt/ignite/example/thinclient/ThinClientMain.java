/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.sbt.ignite.example.thinclient;

import java.util.List;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

/**
 */
public class ThinClientMain {
    public static final String INSERT_QRY = "INSERT INTO ADDR.Address(_KEY, name, zip) VALUES(?, ?, ?)";

    public static final String SELECT_QRY = "SELECT _KEY, name, zip FROM ADDR.Address";

    public static void main(String[] args) throws Exception {
        try (IgniteClient igniteClient = 
                 Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))) {

            createCache(igniteClient);

            ClientCache<String, Address> cache = igniteClient.cache("Addresses");

            printQE(cache);

            putOK(cache);

            putError(cache);

            insertOK(igniteClient);

            insertError(igniteClient);

            selectAll(igniteClient);
        }
    }

    private static void createCache(IgniteClient client) {
        ClientCacheConfiguration ccfg = new ClientCacheConfiguration();
        
        ccfg.setName("compatibilityCheck");
        
        ccfg.setQueryEntities(new QueryEntity()
            .setKeyFieldName("ID")
            .setValueFieldName("name")
            .addQueryField("ID", "java.lang.Long", "ID")
            .addQueryField("name", "java.lang.String", "name"));
        
        client.getOrCreateCache(ccfg);

        printQE(client.getOrCreateCache("compatibilityCheck"));
    }

    private static void printQE(ClientCache<String, Address> cache) {
        QueryEntity[] entities = cache.getConfiguration().getQueryEntities();

        for (QueryEntity entity : entities) {
            System.out.println(cache.getName() + ".Entity = " + entity);
        }
    }

    private static void putOK(ClientCache<String,Address> cache) {
        cache.put("PUT", new Address("ADDR", 127000));

        System.out.println(">>> VALUE PUT OK");
    }

    private static void putError(ClientCache<String, Address> cache) {
        try {
            cache.put("1", new Address("Long Address!", 94612));
            
            throw new IllegalStateException("This put should fail!");
        } catch (Exception e) {
            //Ignore.
        }
    }

    private static void insertOK(IgniteClient igniteClient) {
        execSQL(igniteClient, INSERT_QRY, "INS", "ADDR2", 127001);

        System.out.println(">>> VALUE INSERT OK");
    }

    private static void insertError(IgniteClient igniteClient) {
        try {
            execSQL(igniteClient, INSERT_QRY, "2", "Long Address!", 127001);

            throw new IllegalStateException("This put should fail!");
        } catch (Exception e) {
            //Ignore.
        }
    }

    private static void selectAll(IgniteClient igniteClient) {
        FieldsQueryCursor<List<?>> res = execSQL(igniteClient, SELECT_QRY);

        for(Object r: res) {
            System.out.println(r);
        }
    }

    private static FieldsQueryCursor<List<?>> execSQL(IgniteClient client, String sql, Object... args) {
        return client.query(new SqlFieldsQuery(sql)
            .setArgs(args));
    }
}
