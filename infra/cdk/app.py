import aws_cdk as cdk
from cdk.cdk_stack import GlobalPartnersStack

app = cdk.App()

GlobalPartnersStack(
    app, "GlobalPartnersStack",
    env=cdk.Environment(
        account="426159171194",
        region="us-east-1",
    ),
    description="GlobalPartners Restaurant Analytics Pipeline",
)

app.synth()