# cloud-automation-toolkit
Cloud Automation Scripts

Cloud Automation Toolkit has scripts for Auto Scaling and On-Demand Scaling of Cloudera clusters on Cloud. The result of the script execution is reflected in the Cloudera Director UI.

About

This repository contains the source code of the Cloud Automation Toolkit in Java that interacts with the Director Server API. This can be used to grow and shrink CDH clusters managed by Cloudera Manager on a cloud infrastructure.

This folder contains a a set of classes for auto scaling or on-demand scaling of CDH clusters.

You can run them from an IDE or via Maven like this:

    mvn compile exec:java -Dexec.mainClass="com.cloudera.director.toolkit.GrowOrShrinkCluster" \
        -Dexec.args="--admin-username admin --admin-password --server \"http://localhost:7189\" --config cluster.ini"

    OR

    mvn compile exec:java -Dexec.mainClass="com.cloudera.director.samples.AutoScaleClusterApp" \
        -Dexec.args="--admin-username admin --admin-password --server \"http://localhost:7189\" --config cluster.ini"
        
    OR 
    
    mvn compile exec:java -Dexec.mainClass="com.cloudera.director.samples.DynamicScaleClusterApp" \
        -Dexec.args="--admin-username admin --admin-password --server \"http://localhost:7189\" --config cluster.ini"


Important notice

Copyright © 2015 Cloudera, Inc. Licensed under the Apache License, Version 2.0.

Cloudera, the Cloudera logo, and any other product or service names or slogans contained in this document are trademarks of Cloudera and its suppliers or licensors, and may not be copied, imitated or used, in whole or in part, without the prior written permission of Cloudera or the applicable trademark holder.

Hadoop and the Hadoop elephant logo are trademarks of the Apache Software Foundation. All other trademarks, registered trademarks, product names and company names or logos mentioned in this document are the property of their respective owners. Reference to any products, services, processes or other information, by trade name, trademark, manufacturer, supplier or otherwise does not constitute or imply endorsement, sponsorship or recommendation thereof by us.

Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this document may be reproduced, stored in or introduced into a retrieval system, or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, or otherwise), or for any purpose, without the express written permission of Cloudera.

Cloudera may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this document. Except as expressly provided in any written license agreement from Cloudera, the furnishing of this document does not give you any license to these patents, trademarks, copyrights, or other intellectual property. For information about patents covering Cloudera products, see http://tiny.cloudera.com/patents.

The information in this document is subject to change without notice. Cloudera shall not be liable for any damages resulting from technical errors or omissions which may be present in this document, or from use of this document.
