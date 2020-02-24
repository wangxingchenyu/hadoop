package com.zhileiedu.hadoop.rsa;

import org.apache.commons.net.util.Base64;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * @Author: wzl
 * @Date: 2020/2/24 16:56
 */
public class T {
	public static void main(String[] args) throws NoSuchAlgorithmException {
		// initKey();
		Map<String, String> keys = RSAUtils.createKeys(512);
	}


	public static void initKey() throws NoSuchAlgorithmException {
		KeyPairGenerator keyPairGenerator;

		keyPairGenerator = KeyPairGenerator.getInstance("RSA");
		keyPairGenerator.initialize(512);
		KeyPair keyPair = keyPairGenerator.generateKeyPair();

		String publicKey = Base64.encodeBase64String(keyPair.getPublic().getEncoded());
		String privateKey = Base64.encodeBase64String(keyPair.getPrivate().getEncoded());
		System.out.println("公钥="+publicKey);
		System.out.println("私钥="+privateKey);
	}

}
