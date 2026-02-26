package com.gamesofts.osstimeagent.instrument.asm;

import com.gamesofts.osstimeagent.util.AgentLog;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

import java.util.ArrayList;
import java.util.List;

public final class OssAsmPatcher {
    private static final String BRIDGE_OWNER = "com/gamesofts/osstimeagent/bridge/OssTimeBridge";
    private static final String CLS_OSS_OPERATION = "com/aliyun/oss/internal/OSSOperation";
    private static final String CLS_SERVICE_CLIENT = "com/aliyun/oss/common/comm/ServiceClient";
    private static final String CLS_CLIENT_CONFIGURATION = "com/aliyun/oss/ClientConfiguration";
    private static final String CLS_REQUEST_MESSAGE = "com/aliyun/oss/common/comm/RequestMessage";
    private static final String CLS_RESPONSE_MESSAGE = "com/aliyun/oss/common/comm/ResponseMessage";

    private OssAsmPatcher() {
    }

    private static boolean isShouldRetryMethod(String name, String desc) {
        return "shouldRetry".equals(name) && isShouldRetryDescriptor(desc);
    }

    private static boolean isShouldRetryDescriptor(String desc) {
        Type[] args;
        Type ret;
        try {
            args = Type.getArgumentTypes(desc);
            ret = Type.getReturnType(desc);
        } catch (Throwable t) {
            return false;
        }
        if (ret.getSort() != Type.BOOLEAN || args.length != 5) {
            return false;
        }
        return isObjectType(args[0], "java/lang/Exception")
                && isObjectType(args[1], CLS_REQUEST_MESSAGE)
                && isObjectType(args[2], CLS_RESPONSE_MESSAGE)
                && args[3].getSort() == Type.INT
                && args[4].getSort() == Type.OBJECT;
    }

    private static boolean isSendRequestImplMethod(String name, String desc) {
        if (!"sendRequestImpl".equals(name)) {
            return false;
        }
        Type[] args;
        Type ret;
        try {
            args = Type.getArgumentTypes(desc);
            ret = Type.getReturnType(desc);
        } catch (Throwable t) {
            return false;
        }
        return args.length == 2
                && isObjectType(args[0], CLS_REQUEST_MESSAGE)
                && isObjectType(args[1], "com/aliyun/oss/common/comm/ExecutionContext")
                && isObjectType(ret, "com/aliyun/oss/common/comm/ResponseMessage");
    }

    private static boolean isObjectType(Type type, String internalName) {
        return type != null && type.getSort() == Type.OBJECT && internalName.equals(type.getInternalName());
    }

    public static byte[] patch(String className, byte[] classfileBuffer, PatchStats stats) {
        ClassReader reader = new ClassReader(classfileBuffer);
        ClassWriter writer = new ClassWriter(reader, ClassWriter.COMPUTE_MAXS);
        ClassVisitor visitor;
        if (CLS_OSS_OPERATION.equals(className)) {
            visitor = new OssOperationVisitor(writer, stats);
        } else if (CLS_SERVICE_CLIENT.equals(className)) {
            visitor = new ServiceClientVisitor(writer, stats);
        } else if (CLS_CLIENT_CONFIGURATION.equals(className)) {
            visitor = new ClientConfigurationVisitor(writer, stats);
        } else {
            return null;
        }
        reader.accept(visitor, 0);
        if (!stats.classModified) {
            return null;
        }
        return writer.toByteArray();
    }

    public static final class PatchStats {
        public boolean classModified;
        public boolean tickOffsetPatched;
        public boolean serviceClientRetryPatched;
        public boolean serviceClientPreSyncBeforeSignPatched;
        public boolean serviceClientResignRetryPatched;
        public boolean clientConfigClockSkewPatched;
        public boolean clientConfigTickOffsetHookPatched;
    }

    private static final class OssOperationVisitor extends ClassVisitor {
        private final PatchStats stats;

        private OssOperationVisitor(ClassVisitor cv, PatchStats stats) {
            super(Opcodes.ASM5, cv);
            this.stats = stats;
        }

        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
            if ("createSigner".equals(name)) {
                return new MethodVisitor(Opcodes.ASM5, mv) {
                    public void visitMethodInsn(int opcode, String owner, String mName, String mDesc, boolean itf) {
                        if (opcode == Opcodes.INVOKEVIRTUAL
                                && CLS_CLIENT_CONFIGURATION.equals(owner)
                                && "getTickOffset".equals(mName)
                                && "()J".equals(mDesc)) {
                            super.visitMethodInsn(opcode, owner, mName, mDesc, itf);
                            super.visitMethodInsn(Opcodes.INVOKESTATIC, BRIDGE_OWNER,
                                    "resolveTickOffsetMillis", "(J)J", false);
                            stats.classModified = true;
                            stats.tickOffsetPatched = true;
                            return;
                        }
                        super.visitMethodInsn(opcode, owner, mName, mDesc, itf);
                    }
                };
            }
            return mv;
        }
    }

    private static final class ServiceClientVisitor extends ClassVisitor {
        private final PatchStats stats;

        private ServiceClientVisitor(ClassVisitor cv, PatchStats stats) {
            super(Opcodes.ASM5, cv);
            this.stats = stats;
        }

        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
            if (isShouldRetryMethod(name, desc)) {
                return new ServiceClientShouldRetryMethodVisitor(mv, stats);
            }
            if (isSendRequestImplMethod(name, desc)) {
                return new ServiceClientSendRequestImplMethodNode(access, name, desc, signature, exceptions, mv, stats);
            }
            return mv;
        }
    }

    private static final class ServiceClientShouldRetryMethodVisitor extends MethodVisitor {
        private final PatchStats stats;

        private ServiceClientShouldRetryMethodVisitor(MethodVisitor mv, PatchStats stats) {
            super(Opcodes.ASM5, mv);
            this.stats = stats;
        }

        public void visitCode() {
            super.visitCode();

            Label fallthrough = new Label();

            // if (!(exception instanceof OSSException)) goto fallthrough;
            super.visitVarInsn(Opcodes.ALOAD, 1);
            super.visitTypeInsn(Opcodes.INSTANCEOF, "com/aliyun/oss/OSSException");
            super.visitJumpInsn(Opcodes.IFEQ, fallthrough);

            // if (!"RequestTimeTooSkewed".equals(((OSSException)exception).getErrorCode())) goto fallthrough;
            super.visitLdcInsn("RequestTimeTooSkewed");
            super.visitVarInsn(Opcodes.ALOAD, 1);
            super.visitTypeInsn(Opcodes.CHECKCAST, "com/aliyun/oss/OSSException");
            super.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "com/aliyun/oss/OSSException",
                    "getErrorCode", "()Ljava/lang/String;", false);
            super.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/String",
                    "equals", "(Ljava/lang/Object;)Z", false);
            super.visitJumpInsn(Opcodes.IFEQ, fallthrough);

            // if (retries != 0) goto fallthrough;
            super.visitVarInsn(Opcodes.ILOAD, 4);
            super.visitJumpInsn(Opcodes.IFNE, fallthrough);

            // if (!request.isRepeatable()) goto fallthrough;
            super.visitVarInsn(Opcodes.ALOAD, 2);
            super.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "com/aliyun/oss/common/comm/RequestMessage",
                    "isRepeatable", "()Z", false);
            super.visitJumpInsn(Opcodes.IFEQ, fallthrough);

            // Force one immediate retry after SDK clock-skew adjustment.
            super.visitInsn(Opcodes.ICONST_1);
            super.visitInsn(Opcodes.IRETURN);

            super.visitLabel(fallthrough);
            stats.classModified = true;
            stats.serviceClientRetryPatched = true;
        }
    }

    private static final class ServiceClientSendRequestImplMethodNode extends MethodNode {
        private final MethodVisitor downstream;
        private final PatchStats stats;
        private int retryIndex = -1;

        private ServiceClientSendRequestImplMethodNode(int access, String name, String desc, String signature,
                                                       String[] exceptions, MethodVisitor downstream, PatchStats stats) {
            super(Opcodes.ASM5, access, name, desc, signature, exceptions);
            this.downstream = downstream;
            this.stats = stats;
        }

        public void visitEnd() {
            resolveRetryLocalIndex();
            patchBeforeInitialSignCall();
            patchHandleRequestCalls();
            accept(downstream);
        }

        private void resolveRetryLocalIndex() {
            for (AbstractInsnNode n = instructions.getFirst(); n != null; n = n.getNext()) {
                if (!(n instanceof MethodInsnNode)) {
                    continue;
                }
                MethodInsnNode mi = (MethodInsnNode) n;
                if (!isShouldRetryInvoke(mi)) {
                    continue;
                }
                int idx = findRetryIndexNearCall(mi);
                if (idx >= 0) {
                    retryIndex = idx;
                    return;
                }
            }
        }

        private int findRetryIndexNearCall(MethodInsnNode call) {
            List vars = new ArrayList();
            AbstractInsnNode p = call.getPrevious();
            int steps = 0;
            while (p != null && steps < 16) {
                if (p instanceof VarInsnNode) {
                    vars.add((VarInsnNode) p);
                }
                if (isBoundaryInsn(p)) {
                    break;
                }
                p = p.getPrevious();
                steps++;
            }
            int i;
            for (i = 0; i < vars.size(); i++) {
                VarInsnNode v = (VarInsnNode) vars.get(i);
                if (v.getOpcode() == Opcodes.ILOAD) {
                    return v.var;
                }
            }
            return -1;
        }

        private boolean isBoundaryInsn(AbstractInsnNode n) {
            int op = n.getOpcode();
            return op == Opcodes.IRETURN || op == Opcodes.ARETURN || op == Opcodes.RETURN
                    || op == Opcodes.ATHROW || op == Opcodes.GOTO;
        }

        private void patchHandleRequestCalls() {
            boolean resignSkippedWarned = false;
            for (AbstractInsnNode n = instructions.getFirst(); n != null; n = n.getNext()) {
                if (!(n instanceof MethodInsnNode)) {
                    continue;
                }
                MethodInsnNode mi = (MethodInsnNode) n;
                if (!isHandleRequestInvoke(mi)) {
                    continue;
                }
                InsnList inject = new InsnList();

                if (retryIndex >= 0) {
                    // Re-sign retries after SDK adjusts tickOffset in catch path.
                    inject.add(new VarInsnNode(Opcodes.ALOAD, 0));
                    inject.add(new VarInsnNode(Opcodes.ALOAD, 1));
                    inject.add(new VarInsnNode(Opcodes.ALOAD, 2));
                    inject.add(new VarInsnNode(Opcodes.ILOAD, retryIndex));
                    inject.add(new MethodInsnNode(Opcodes.INVOKESTATIC, BRIDGE_OWNER,
                            "resignForRetry", "(Ljava/lang/Object;Ljava/lang/Object;Ljava/lang/Object;I)V", false));
                    stats.classModified = true;
                    stats.serviceClientResignRetryPatched = true;
                } else if (!resignSkippedWarned) {
                    resignSkippedWarned = true;
                    AgentLog.warn("ServiceClient.sendRequestImpl resign patch skipped: retries local index unresolved (sdk may be unsupported 3.x variant)");
                }

                instructions.insertBefore(mi, inject);
            }
        }

        private void patchBeforeInitialSignCall() {
            for (AbstractInsnNode n = instructions.getFirst(); n != null; n = n.getNext()) {
                if (!(n instanceof MethodInsnNode)) {
                    continue;
                }
                MethodInsnNode mi = (MethodInsnNode) n;
                if (!isRequestSignerSignInvoke(mi)) {
                    continue;
                }
                InsnList inject = new InsnList();
                inject.add(new VarInsnNode(Opcodes.ALOAD, 0));
                inject.add(new VarInsnNode(Opcodes.ALOAD, 1));
                inject.add(new VarInsnNode(Opcodes.ALOAD, 2));
                inject.add(new MethodInsnNode(Opcodes.INVOKESTATIC, BRIDGE_OWNER,
                        "beforeInitialSign", "(Ljava/lang/Object;Ljava/lang/Object;Ljava/lang/Object;)V", false));
                instructions.insertBefore(mi, inject);
                stats.classModified = true;
                stats.serviceClientPreSyncBeforeSignPatched = true;
                return;
            }
            AgentLog.warn("ServiceClient.sendRequestImpl pre-sync patch skipped: initial signer.sign invocation unresolved");
        }
    }

    private static boolean isShouldRetryInvoke(MethodInsnNode mi) {
        return mi != null
                && "shouldRetry".equals(mi.name)
                && CLS_SERVICE_CLIENT.equals(mi.owner)
                && isShouldRetryDescriptor(mi.desc);
    }

    private static boolean isHandleRequestInvoke(MethodInsnNode mi) {
        if (mi == null || !"handleRequest".equals(mi.name) || !CLS_SERVICE_CLIENT.equals(mi.owner)) {
            return false;
        }
        Type[] args = Type.getArgumentTypes(mi.desc);
        if (args.length < 1) {
            return false;
        }
        if (Type.getReturnType(mi.desc).getSort() != Type.VOID) {
            return false;
        }
        Type a0 = args[0];
        return a0.getSort() == Type.OBJECT && CLS_REQUEST_MESSAGE.equals(a0.getInternalName());
    }

    private static boolean isRequestSignerSignInvoke(MethodInsnNode mi) {
        if (mi == null || !"sign".equals(mi.name)) {
            return false;
        }
        Type[] args;
        Type ret;
        try {
            args = Type.getArgumentTypes(mi.desc);
            ret = Type.getReturnType(mi.desc);
        } catch (Throwable t) {
            return false;
        }
        return ret.getSort() == Type.VOID
                && args.length == 1
                && isObjectType(args[0], CLS_REQUEST_MESSAGE);
    }

    private static final class ClientConfigurationVisitor extends ClassVisitor {
        private final PatchStats stats;

        private ClientConfigurationVisitor(ClassVisitor cv, PatchStats stats) {
            super(Opcodes.ASM5, cv);
            this.stats = stats;
        }

        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
            if ("setTickOffset".equals(name) && "(J)V".equals(desc)) {
                return new ClientConfigurationSetTickOffsetVisitor(mv, stats);
            }
            return mv;
        }
    }

    private static final class ClientConfigurationSetTickOffsetVisitor extends MethodVisitor {
        private final PatchStats stats;

        private ClientConfigurationSetTickOffsetVisitor(MethodVisitor mv, PatchStats stats) {
            super(Opcodes.ASM5, mv);
            this.stats = stats;
        }

        public void visitInsn(int opcode) {
            if (opcode == Opcodes.RETURN) {
                super.visitVarInsn(Opcodes.ALOAD, 0);
                super.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "com/aliyun/oss/ClientConfiguration",
                        "getTickOffset", "()J", false);
                super.visitMethodInsn(Opcodes.INVOKESTATIC, BRIDGE_OWNER,
                        "onConfigTickOffsetUpdatedFromSdk", "(J)V", false);
                stats.classModified = true;
                stats.clientConfigTickOffsetHookPatched = true;
            }
            super.visitInsn(opcode);
        }
    }

}
