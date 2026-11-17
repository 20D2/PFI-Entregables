# ============================================================
#  MikroTik receptor del Sitio A (RB951Ui-2HnD, RouterOS 6.49.18)
#  Configuracion del proyecto GPON + IoT
#  Extracto filtrado del export. Se omiten datos identificatorios
#  del equipo (numero de serie, ID de software, MAC de administracion)
#  y los elementos que no intervienen en el proyecto.
#  La ONU entrega la VLAN 200 sin etiquetar: el cliente DHCP corre
#  directamente sobre el puerto fisico (ether2), sin subinterfaz VLAN.
#  Canal Wi-Fi 1 (2412 MHz, 20 MHz): distinto del servidor (6) y del Sitio B (11).
#  Es un extracto de referencia, no un backup restaurable.
# ============================================================
/interface bridge
add name=bridge
/interface list
add name=WAN
add name=LAN
/interface wireless security-profiles
add authentication-types=wpa2-psk mode=dynamic-keys name=wifi-security \
    wpa2-pre-shared-key=********
/interface wireless
set [ find default-name=wlan1 ] band=2ghz-b/g/n \
    country=argentina disabled=no distance=indoors frequency=2412 \
    installation=indoor mode=ap-bridge security-profile=wifi-security \
    ssid=MQTT-sitioA wireless-protocol=802.11 wps-mode=disabled
/ip pool
add name=default-dhcp ranges=192.168.88.10-192.168.88.254
/ip dhcp-server
add address-pool=default-dhcp disabled=no interface=bridge name=defconf
/interface bridge port
add bridge=bridge interface=ether3
add bridge=bridge interface=ether4
add bridge=bridge interface=ether5
add bridge=bridge interface=wlan1
/interface list member
add interface=bridge list=LAN
add interface=ether2 list=WAN
/ip address
add address=192.168.88.1/24 interface=bridge network=192.168.88.0
/ip dhcp-client
add disabled=no interface=ether2
/ip dhcp-server network
add address=192.168.88.0/24 dns-server=192.168.88.1 gateway=192.168.88.1
/ip dns
set allow-remote-requests=yes
/ip firewall filter
add action=accept chain=input connection-state=established,related,untracked
add action=drop chain=input connection-state=invalid
add action=accept chain=input protocol=icmp
add action=drop chain=input in-interface-list=!LAN
add action=accept chain=forward connection-state=established,related,untracked
add action=drop chain=forward connection-state=invalid
add action=drop chain=forward connection-nat-state=!dstnat connection-state=new in-interface-list=WAN
/ip firewall nat
add action=masquerade chain=srcnat out-interface-list=WAN
