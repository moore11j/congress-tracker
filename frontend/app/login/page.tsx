import { LoginRegisterPanelDeferred } from "@/components/auth/LoginRegisterPanelDeferred";
import type { Metadata } from "next";

export const dynamic = "force-static";

export const metadata: Metadata = {
  title: "Login | Walnut Markets",
  robots: {
    index: false,
    follow: true,
  },
};

export default function LoginPage() {
  return <LoginRegisterPanelDeferred />;
}
