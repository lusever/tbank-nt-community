# T-Bank TLS certificates

T-Bank requires clients to trust the Russian Trusted CA chain in addition to
the platform roots. The adapter vendors both certificates and adds them to the
normal rustls root store; it does not disable certificate or hostname checks.

Sources:

- T-Bank migration guide: <https://developer.tbank.ru/ecosystembundle/intro/integration/migration-russian-trusted-ca>
- Official certificate distribution: <https://www.gosuslugi.ru/crt>

Pinned certificates:

| File | Subject | SHA-256 fingerprint | Valid until |
| --- | --- | --- | --- |
| `russian_trusted_root_ca.pem` | Russian Trusted Root CA | `D2:6D:2D:02:31:B7:C3:9F:92:CC:73:85:12:BA:54:10:35:19:E4:40:5D:68:B5:BD:70:3E:97:88:CA:8E:CF:31` | 2032-02-27 |
| `russian_trusted_sub_ca.pem` | Russian Trusted Sub CA | `BB:BD:E2:10:3E:79:0B:99:9E:C6:2B:D0:3C:F6:25:A5:A2:E7:C3:16:E1:0A:FE:6A:49:0E:ED:EA:D8:B3:FD:9B` | 2027-03-06 |

Before replacing a certificate, verify its full fingerprint and validate the
subordinate certificate against the pinned root with OpenSSL. Plan the Sub CA
rotation before its expiry; an expired certificate must not be bypassed with an
insecure TLS mode.
