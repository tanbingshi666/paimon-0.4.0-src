/*
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

public class TestIfStatementRewrite2 {
    public void myFun1(int[] a, int[] b) throws RuntimeException {

        if (a[0] == 0) {
            myFun1_0_0_rewriteGroup5(a, b);
        } else if(a[1] == 22) {
            myFun1_0_11_12(a, b);
        } else {
            myFun1_0_11_13(a, b);
        }
    }

    void myFun1_0_0_1_2_3_5_7_8(int[] a, int[] b) throws RuntimeException {
        a[4] = b[4];
        a[44] = b[44];
    }

    void myFun1_0_0_1_2_3_5_7_9(int[] a, int[] b) throws RuntimeException {
        a[5] = 5;
        System.out.println("nothing");
    }

    void myFun1_0_0_rewriteGroup1_2_rewriteGroup4(int[] a, int[] b) throws RuntimeException {
        a[1] = 1;
        if (a[2] == 0) {
            a[2] = 1;
        } else if (a[3] == 0) {
            myFun1_0_0_1_2_3_5_6(a, b);
        } else if (a[4] == 0) {
            myFun1_0_0_1_2_3_5_7_8(a, b);
        } else {
            myFun1_0_0_1_2_3_5_7_9(a, b);
        }
    }

    void myFun1_0_0_1_10(int[] a, int[] b) throws RuntimeException {
        a[1] = b[1];
        a[2] = b[2];
    }

    void myFun1_0_0_rewriteGroup5(int[] a, int[] b) throws RuntimeException {
        a[0] = 1;
        if (a[1] == 0) {
            myFun1_0_0_rewriteGroup1_2_rewriteGroup4(a, b);
        } else {
            myFun1_0_0_1_10(a, b);
        }
    }

    void myFun1_0_0_1_2_3_5_6(int[] a, int[] b) throws RuntimeException {
        a[3] = b[3];
        a[33] = b[33];
    }

    void myFun1_0_11_13(int[] a, int[] b) throws RuntimeException {
        a[0] = b[0];
        a[1] = b[1];
        a[2] = b[2];
    }

    void myFun1_0_11_12(int[] a, int[] b) throws RuntimeException {
        a[1] = b[12];
        a[2] = b[22];
    }

}
