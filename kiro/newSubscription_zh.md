# Kiro

## 登录

可以選擇:

    Social login（社交账号登录）：
        Google
        Github
    Builder ID
    Organization（组织）

![](./Capture-2026-08-10_15-58-35.jpeg)

如果想试用 Free plan（免费计划），客户可以使用社交账号登录，或者注册一个同样免费的 Builder ID。
    https://aws.amazon.com/builder/


## Models（模型）

使用 Builder ID Free plan 时，可以访问部分模型，可用的模型可能因地区/IP 不同而有所差异。

![](./Screenshot_2026-08-10_17-04-46.png)

![](./Capture-2026-08-10_17-06-45.jpg)


## 使用付费计划

通过 AWS Account 付费/订阅可以获得更多 credits 和更多模型。我们可以在 AWS 上订阅 Kiro。计划如下：

![](./Capture-2026-08-10_11-33-22.jpg)

要订阅的话，我们需要先在 AWS 上启用 Kiro：

### 启用 Kiro

访问 Kiro：https://us-east-1.console.aws.amazon.com/amazonq/developer/home?region=us-east-1

点击 "Sign up for Kiro"，然后选择 "IAM Identity Center"。

![](./Capture-2026-08-10_11-26-05.jpg)


按照步骤设置 IAM Identity Center instance（如果还没有的话）。
    选择 'Enable'

![](./Capture-2026-08-10_11-26-36.jpg)


简单设置的话，选择 Single region 即可。

![](./Capture-2026-08-10_11-27-03.jpg)

如果需要高可用性，选择 Multi-Region。

![](./Capture-2026-08-10_11-27-23.jpg)


完成 IAM Identity Center 设置后，启用 Kiro。

![](./Capture-2026-08-10_11-28-21.jpg)



在新设置的 IAM Identity Center 中添加用户，


![](./Capture-2026-08-10_11-31-20.jpg)

![](./Capture-2026-08-10_11-31-10.jpg)



成功添加用户后，在 Kiro 中添加用户并选择计划

![](./Capture-2026-08-10_17-19-34.jpg)

![](./Capture-2026-08-10_11-33-22.jpg)


选择在 IAM Identity Center 中新创建的用户

![](./Capture-2026-08-10_17-21-20.jpg)



记下 Sign URL

![](./Capture-2026-08-10_17-22-38.jpg)



现在从免费计划切换到付费计划：

退出之前的登录，通过 "Your organization" 登录，如果出现选项请确保选择 "Sign in Via IAM Identity Center"。

![](./Capture-2026-08-10_17-24-03.jpg)

![](./Capture-2026-08-10_17-29-07.jpg)


提供 sign in URL 和 region。

![](./Capture-2026-08-10_17-25-35.jpg)

使用在 IAM Identity Center 中设置的用户名/密码完成登录


现在计划拥有更多 Credits 了。

![](./Capture-2026-08-10_17-32-56.jpg)
