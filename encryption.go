package utils

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"github.com/sirupsen/logrus"
)

//使用私钥加签，生成sign字符串
func Sha1WithRsa(content string, privateKey []byte) string {
	block2, _ := pem.Decode(privateKey) //piravteKey为私钥文件的字节数组
	if block2 == nil {
		logrus.Errorln("加签，decode private_key failed")
		return ""
	}

	//priv即私钥对象,block2.Bytes是私钥的字节流
	priv, err := x509.ParsePKCS8PrivateKey(block2.Bytes)

	if err != nil {
		logrus.Errorln("加签，无法还原私钥", err)
		return ""
	}

	h := crypto.Hash.New(crypto.SHA1)
	h.Write([]byte(content))
	hashed := h.Sum(nil)

	signature2, err := rsa.SignPKCS1v15(rand.Reader, priv.(*rsa.PrivateKey), crypto.SHA1, hashed) //签名

	return base64Encode(signature2)
}

//公钥加密
func PublicEncrypt(data string, publicKey string) (string, error) {
	block, _ := pem.Decode([]byte(publicKey))
	if block == nil {
		return "", errors.New("public key error")
	}
	// 解析公钥
	pubInterface, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return "", err
	}
	// 类型断言
	pub := pubInterface.(*rsa.PublicKey)
	partLen := pub.N.BitLen()/8 - 11
	chunks := split([]byte(data), partLen)
	buffer := bytes.NewBufferString("")
	for _, chunk := range chunks {
		bytes, err := rsa.EncryptPKCS1v15(rand.Reader, pub, chunk)
		if err != nil {
			return "", err
		}
		buffer.Write(bytes)
	}

	return base64Encode(buffer.Bytes()), nil
}

// 私钥解密
func PrivateDecrypt(encrypted string, privateKey string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(encrypted)

	block, _ := pem.Decode([]byte(privateKey))
	if block == nil {
		return "", errors.New("private key error")
	}
	//解析PKCS8格式的私钥
	priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return "", err
	}

	privInterface := priv.(*rsa.PrivateKey)
	chunks := split([]byte(raw), privInterface.N.BitLen()/8)
	buffer := bytes.NewBufferString("")
	for _, chunk := range chunks {
		decrypted, err := rsa.DecryptPKCS1v15(rand.Reader, privInterface, chunk)
		if err != nil {
			return "分段解密失败", err
		}
		buffer.Write(decrypted)
	}

	return buffer.String(), err
}

//验签
func verify(sign []byte, hashed []byte, publicKey string) bool {
	block, _ := pem.Decode([]byte(publicKey))
	if block == nil {
		logrus.Infoln("验签，block 空")
		return false
	}
	pubInterface, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		logrus.Infoln("验签，还原公钥错误")
		return false
	}
	pub := pubInterface.(*rsa.PublicKey) //pub:公钥对象
	err = rsa.VerifyPKCS1v15(pub, crypto.SHA1, hashed, []byte(base64Decode(string(sign))))

	if err != nil {
		return false
	} else {
		return true
	}
}

//base64加密
func base64Encode(src []byte) string {
	encodeString := base64.StdEncoding.EncodeToString(src)
	return encodeString
}

//base64解密
func base64Decode(src string) string {
	decodeBytes, _ := base64.StdEncoding.DecodeString(src)
	return string(decodeBytes)
}

//分段加解密私有方法
func split(buf []byte, lim int) [][]byte {
	var chunk []byte
	chunks := make([][]byte, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:len(buf)])
	}
	return chunks
}
