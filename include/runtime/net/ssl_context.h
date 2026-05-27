// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/base/noncopyable.h"
#include <openssl/ssl.h> // SSL_CTX / SSL / TLS_server_method ...
#include <stdexcept>
#include <string>


namespace runtime::net {

// Server-side TLS configuration shared across all connections.
// Owns the SSL_CTX and advertises h2/http/1.1 via ALPN.
class SslContext : public runtime::base::NonCopyable {
public:
  SslContext() {
    ctx_ = SSL_CTX_new(TLS_server_method());
    if (!ctx_) throw std::runtime_error("SSL_CTX_new failed");

    SSL_CTX_set_alpn_select_cb(ctx_, AlpnSelectCb, nullptr);
  }
  ~SslContext() { 
    if (ctx_) {
        SSL_CTX_free(ctx_);
    }
   }

  void LoadCertAndKey(const std::string& cert_file, const std::string& key_file) {
    if (SSL_CTX_use_certificate_file(ctx_, cert_file.c_str(), SSL_FILETYPE_PEM) != 1)
      throw std::runtime_error("load cert failed");
    if (SSL_CTX_use_PrivateKey_file(ctx_, key_file.c_str(), SSL_FILETYPE_PEM) != 1)
      throw std::runtime_error("load key failed");
    if (SSL_CTX_check_private_key(ctx_) != 1) 
      throw std::runtime_error("cert and key not match");
  }
  // Caller takes ownership of the returned SSL*.
  SSL* NewSsl() const { return SSL_new(ctx_); }
private:
  // Prefers h2 over http/1.1 in ALPN negotiation.
  static int AlpnSelectCb(SSL*,
                          const unsigned char** out,
                          unsigned char* outlen,
                          const unsigned char* in,
                          unsigned int inlen,
                          void*) {
    // Wire format: length-prefixed list.
    // "\x02h2"       = 2 bytes, "h2"
    // "\x08http/1.1" = 8 bytes, "http/1.1"
    static const unsigned char kH2[] = "\x02h2";
    static const unsigned char kHttp11[] = "\x08http/1.1";

    if (SSL_select_next_proto((unsigned char**)out, outlen,
                              kH2, sizeof(kH2) - 1,
                              in, inlen) == OPENSSL_NPN_NEGOTIATED) {
      return SSL_TLSEXT_ERR_OK;
    }

    if (SSL_select_next_proto((unsigned char**)out, outlen,
                              kHttp11, sizeof(kHttp11) - 1,
                              in, inlen) == OPENSSL_NPN_NEGOTIATED) {
      return SSL_TLSEXT_ERR_OK;
    }

    return SSL_TLSEXT_ERR_NOACK;
  }
private:
  SSL_CTX* ctx_{nullptr};
};
} // namespace runtime::net