/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.auth;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;

/**
 * Client factory for PLAIN authentication. This is simplified copy of Java class ClientFactoryImpl
 * from which I removed "EXTERNAL", "CRAM-MD5" authentication types. I have to copy sources because
 * ClientFactoryImpl is not visible outside Java package.
 */
public final class SaslPlainClientFactory implements SaslClientFactory {
    public static final String PLAIN_METHOD = "PLAIN";

    public SaslPlainClientFactory() {
    }

    public SaslClient createSaslClient(String[] mechanisms, String authorizationId, String protocol, String serverName,
                                       Map<String, ?> props, CallbackHandler cbh) throws SaslException {
        for (String mechanism : mechanisms) {
            if (PLAIN_METHOD.equals(mechanism)) {
                Object[] userInfo = this.getUserInfo(authorizationId, cbh);
                return new PlainClient(authorizationId, (String) userInfo[0], (byte[]) userInfo[1]);
            }
        }
        return null;
    }

    public String[] getMechanismNames(Map<String, ?> props) {
        return new String[] { PLAIN_METHOD };
    }

    private Object[] getUserInfo(String authorizationId, CallbackHandler cbh) throws SaslException {
        if (cbh == null) {
            throw new SaslException("Callback handler to get username/password required");
        } else {
            try {
                String userPrompt = "PLAIN authentication id: ";
                String passwdPrompt = "PLAIN password: ";
                NameCallback ncb =
                        authorizationId == null ? new NameCallback(userPrompt) : new NameCallback(userPrompt, authorizationId);
                PasswordCallback pcb = new PasswordCallback(passwdPrompt, false);
                cbh.handle(new Callback[] { ncb, pcb });
                char[] pw = pcb.getPassword();
                byte[] bytepw;
                if (pw != null) {
                    bytepw = (new String(pw)).getBytes(StandardCharsets.UTF_8);
                    pcb.clearPassword();
                } else {
                    bytepw = null;
                }

                String authId = ncb.getName();
                return new Object[] { authId, bytepw };
            } catch (IOException ioException) {
                throw new SaslException("Cannot get password", ioException);
            } catch (UnsupportedCallbackException unsupportedCallbackException) {
                throw new SaslException("Cannot get userid/password", unsupportedCallbackException);
            }
        }
    }
}
