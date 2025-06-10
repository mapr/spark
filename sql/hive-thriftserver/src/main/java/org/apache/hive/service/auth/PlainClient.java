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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * Client for PLAIN authentication method. PlainClient is copy of Java PlainClient class.
 * I have to copy sources because PlainClient is not visible outside Java package.
 */
final class PlainClient implements SaslClient {
    private boolean completed = false;
    private byte[] pw;
    private final String authorizationID;
    private final String authenticationID;

    PlainClient(String authorizationID, String authenticationID, byte[] pw) throws SaslException {
        if (authenticationID != null && pw != null) {
            this.authorizationID = authorizationID;
            this.authenticationID = authenticationID;
            this.pw = pw;
        } else {
            throw new SaslException("PLAIN: authorization ID and password must be specified");
        }
    }

    public String getMechanismName() {
        return "PLAIN";
    }

    public boolean hasInitialResponse() {
        return true;
    }

    public void dispose() throws SaslException {
        this.clearPassword();
    }

    public byte[] evaluateChallenge(byte[] challengeData) throws SaslException {
        if (this.completed) {
            throw new IllegalStateException("PLAIN authentication already completed");
        } else {
            this.completed = true;

            byte[] authz = this.authorizationID != null ? this.authorizationID.getBytes(StandardCharsets.UTF_8) : null;
            byte[] auth = this.authenticationID.getBytes(StandardCharsets.UTF_8);
            byte[] answer = new byte[this.pw.length + auth.length + 2 + (authz == null ? 0 : authz.length)];
            int pos = 0;
            if (authz != null) {
                System.arraycopy(authz, 0, answer, 0, authz.length);
                pos = authz.length;
            }

            byte SEP = 0;
            answer[pos++] = SEP;
            System.arraycopy(auth, 0, answer, pos, auth.length);
            pos += auth.length;
            answer[pos++] = SEP;
            System.arraycopy(this.pw, 0, answer, pos, this.pw.length);
            this.clearPassword();
            return answer;
        }
    }

    public boolean isComplete() {
        return this.completed;
    }

    public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
        if (this.completed) {
            throw new SaslException("PLAIN supports neither integrity nor privacy");
        } else {
            throw new IllegalStateException("PLAIN authentication not completed");
        }
    }

    public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
        if (this.completed) {
            throw new SaslException("PLAIN supports neither integrity nor privacy");
        } else {
            throw new IllegalStateException("PLAIN authentication not completed");
        }
    }

    public Object getNegotiatedProperty(String propName) {
        if (this.completed) {
            return propName.equals("javax.security.sasl.qop") ? "auth" : null;
        } else {
            throw new IllegalStateException("PLAIN authentication not completed");
        }
    }

    private void clearPassword() {
        if (this.pw != null) {
            Arrays.fill(this.pw, (byte) 0);
            this.pw = null;
        }
    }

    protected void finalize() {
        this.clearPassword();
    }
}
