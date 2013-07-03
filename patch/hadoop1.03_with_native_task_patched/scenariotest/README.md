Configure build.xml
=======================
Change build.xml. Set hadoop.home, and hbase.home to make it point to the hadoop folder of your cluster. Typically it is under /usr/lib/hadoop

<property name="hadoop.home" value="/usr/lib/hadoop" />
<property name="hbase.home" value="/usr/lib/hbase" />