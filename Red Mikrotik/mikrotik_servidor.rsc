# ============================================================
#  MikroTik servidor (RB751U-2HnD, RouterOS 6.49.7)
#  Configuracion del proyecto GPON + IoT
#  Extracto filtrado del export. Contiene SOLO lo necesario para:
#    - enlace con la OLT por VLAN 200 (datos) y VLAN 99 (gestion de la OLT)
#    - red Wi-Fi "MQTT-servidor" donde se conecta el equipo servidor
#      (canal 6, 2437 MHz, 20 MHz)
#    - reenvio del trafico MQTT (TCP 1883)
#  Se omiten datos identificatorios del equipo.
#  Es un extracto de referencia, no un backup restaurable.
# ============================================================
/interface bridge
add comment=defconf name=bridge
/interface vlan
add interface=bridge name=vlan99-olt vlan-id=99
add interface=bridge name=vlan200 vlan-id=200
/interface list
add comment=defconf name=WAN
add comment=defconf name=LAN
/interface wireless security-profiles
add authentication-types=wpa2-psk mode=dynamic-keys name=wifi-security \
    wpa2-pre-shared-key=********
/interface wireless
set [ find default-name=wlan1 ] band=2ghz-b/g/n country=argentina disabled=no \
    frequency=2437 installation=indoor mode=ap-bridge \
    security-profile=wifi-security ssid=MQTT-servidor wps-mode=disabled
/ip pool
add name=pool-onus ranges=10.1.1.10-10.1.1.254
add name=dhcp_pool4 ranges=192.168.20.10-192.168.20.200
/ip dhcp-server
add address-pool=pool-onus disabled=no interface=vlan200 name=dhcp-onus
add address-pool=dhcp_pool4 disabled=no interface=bridge name=dhcp1
/interface bridge port
add bridge=bridge comment="Enlace con la OLT" interface=ether3
add bridge=bridge interface=wlan1
/interface bridge vlan
add bridge=bridge tagged=bridge,ether3 vlan-ids=200
/interface list member
add comment=defconf interface=bridge list=LAN
add comment=defconf interface=ether1 list=WAN
add interface=wlan1 list=LAN
/ip address
add address=10.1.1.1/24 interface=vlan200 network=10.1.1.0
add address=10.99.99.1/24 interface=vlan99-olt network=10.99.99.0
add address=192.168.20.1/24 interface=bridge network=192.168.20.0
/ip dhcp-client
add comment=defconf disabled=no interface=ether1
/ip dhcp-server lease
add address=192.168.20.199 comment="Nodo ESP32" mac-address=08:A6:F7:XX:XX:XX \
    server=dhcp1
add address=192.168.20.198 comment="Equipo servidor" mac-address=4C:D5:77:XX:XX:XX \
    server=dhcp1
/ip dhcp-server network
add address=10.1.1.0/24 dns-server=8.8.8.8,1.1.1.1 gateway=10.1.1.1
add address=192.168.20.0/24 dns-server=192.168.20.1 gateway=192.168.20.1
/ip dns
set allow-remote-requests=yes servers=1.1.1.1,8.8.8.8
/ip firewall filter
add action=accept chain=forward comment="MQTT" dst-port=1883 protocol=tcp
add action=accept chain=input comment="defconf: accept established,related,untracked" \
    connection-state=established,related,untracked
add action=accept chain=input comment="defconf: accept ICMP" protocol=icmp
add action=drop chain=input comment="Bloquear acceso de clientes a la conf del router" \
    in-interface=vlan200
/ip firewall nat
add action=masquerade chain=srcnat comment="defconf: masquerade" \
    ipsec-policy=out,none out-interface-list=WAN
/system clock
set time-zone-name=America/Buenos_Aires
