package es.upm.master.zookeeper.simpleExample;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.Dimension;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class UserConsole {
    private JTextField usernameTextField;
    private JButton buttonLogInRegister;
    private JPanel formulario;
    private JTextArea textAreaMessage;
    private JTextField textFieldMessageTo;
    private JButton buttonSendMessage;
    private JButton buttonReadMyMessages;
    private JLabel labelSendMessageTo;
    private JLabel labelMessage;
    private JButton buttonLogOut;
    private JButton buttonQuit;


    public UserConsole() {

        textAreaMessage.setVisible(false);
        textFieldMessageTo.setVisible(false);
        buttonSendMessage.setVisible(false);
        buttonReadMyMessages.setVisible(false);
        labelSendMessageTo.setVisible(false);
        labelMessage.setVisible(false);
        buttonLogOut.setVisible(false);
        buttonQuit.setVisible(false);

        buttonLogInRegister.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ZKWriter zkw = new ZKWriter();
                String clientName = usernameTextField.getText();

                try {
                    zkw.ZKWriter(clientName);
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

                try {
                    zkw.create();
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                try {
                    zkw.goOnline();
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                textAreaMessage.setVisible(true);
                textFieldMessageTo.setVisible(true);
                buttonSendMessage.setVisible(true);
                buttonReadMyMessages.setVisible(true);
                labelSendMessageTo.setVisible(true);
                labelMessage.setVisible(true);
                buttonLogOut.setVisible(true);
                buttonLogInRegister.setVisible(false);
                usernameTextField.setEditable(false);
                buttonQuit.setVisible(false);
            }
        });

        // Button that controls when to SEND a message
        buttonSendMessage.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ZKWriter zkw = new ZKWriter();
                String receiverName = textFieldMessageTo.getText();
                String messageContent = textAreaMessage.getText();
                String clientName = usernameTextField.getText();

                try {
                    zkw.ZKWriter(clientName);
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

                try {
                    zkw.send(receiverName, messageContent);
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (UnsupportedEncodingException e1) {
                    e1.printStackTrace();
                }
            }
        });

        // Button that controls when to READ a message
        buttonReadMyMessages.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ZKWriter zkw = new ZKWriter();
                String clientName = usernameTextField.getText();

                try {
                    zkw.ZKWriter(clientName);
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

                final JFrame frame = new JFrame();
                try {
                    JOptionPane.showMessageDialog(frame.getComponent(0),  zkw.read());
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (UnsupportedEncodingException e1) {
                    e1.printStackTrace();
                }
            }
        });

        // Button that controls when to QUIT
        buttonLogOut.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ZKWriter zkw = new ZKWriter();
                String clientName = usernameTextField.getText();

                try {
                    zkw.ZKWriter(clientName);
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

                try {
                    zkw.goOffline();
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                textAreaMessage.setVisible(false);
                textFieldMessageTo.setVisible(false);
                buttonSendMessage.setVisible(false);
                buttonReadMyMessages.setVisible(false);
                labelSendMessageTo.setVisible(false);
                labelMessage.setVisible(false);
                buttonLogInRegister.setVisible(true);
                buttonLogOut.setVisible(false);
                buttonQuit.setVisible(true);
                usernameTextField.setEditable(true);
            }
        });

        // Button that controls when to QUIT
        buttonQuit.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ZKWriter zkw = new ZKWriter();
                String clientName = usernameTextField.getText();

                try {
                    zkw.ZKWriter(clientName);
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

                try {
                    zkw.zooDisconnect();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                System.exit(0);
            }
        });
    }


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        JFrame frames = new JFrame("ZooApp");
        frames.setPreferredSize(new Dimension(500,300));
        frames.setLocation(500,250);
        frames.setContentPane(new UserConsole().formulario);
        frames.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frames.pack();
        frames.setVisible(true);

    }


}
