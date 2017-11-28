package es.upm.master.zookeeper.simpleExample;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.*;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.Dimension;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class UserConsole{
    private JTextField usernameTextField;
    private JButton buttonLogInRegister;
    private JPanel formulario;
    private JTextArea textAreaMessage;
    private JButton buttonSendMessage;
    private JButton buttonReadMyMessages;
    private JLabel labelSendMessageTo;
    private JLabel labelMessage;
    private JButton buttonLogOut;
    private JButton buttonQuit;
    private JTextArea ConsoleReading;
    private JLabel chatlabel;
    private JTextArea textMessageTo;
    private JComboBox userComboBox;
    private String online = "/System/Online/";

    public UserConsole() {
        ZKWriter zkw = new ZKWriter();

        textAreaMessage.setVisible(false);
        buttonSendMessage.setVisible(false);
        buttonReadMyMessages.setVisible(false);
        labelSendMessageTo.setVisible(false);
        labelMessage.setVisible(false);
        buttonLogOut.setVisible(false);
        buttonQuit.setVisible(false);
        ConsoleReading.setVisible(false);
        chatlabel.setVisible(false);
        textMessageTo.setVisible(false);
        userComboBox.setVisible(false);

        buttonLogInRegister.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String clientName = usernameTextField.getText();

                try {
                    zkw.ZKWriter(clientName, UserConsole.this);
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
                buttonSendMessage.setVisible(true);
                buttonReadMyMessages.setVisible(true);
                labelSendMessageTo.setVisible(true);
                labelMessage.setVisible(true);
                buttonLogOut.setVisible(true);
                buttonLogInRegister.setVisible(false);
                usernameTextField.setEditable(false);
                buttonQuit.setVisible(false);
                textMessageTo.setVisible(true);
                userComboBox.setVisible(true);

            }
        });



        // Button that controls when to SEND a message
        buttonSendMessage.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                //String receiverName = comboMessageTo.getName();
                String receiverName = textMessageTo.getText();
                String messageContent = textAreaMessage.getText();

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

        // Button that controls when to QUIT
        buttonLogOut.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                try {
                    zkw.goOffline();
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                textAreaMessage.setVisible(false);
              //  comboMessageTo.setVisible(false);
                buttonSendMessage.setVisible(false);
                buttonReadMyMessages.setVisible(false);
                labelSendMessageTo.setVisible(false);
                labelMessage.setVisible(false);
                buttonLogInRegister.setVisible(true);
                buttonLogOut.setVisible(false);
                buttonQuit.setVisible(true);
                usernameTextField.setEditable(true);
                textMessageTo.setVisible(false);
                chatlabel.setVisible(false);
                ConsoleReading.setVisible(false);
            }
        });

        // Button that controls when to QUIT
        buttonQuit.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ZKWriter zkw = new ZKWriter();
                String clientName = usernameTextField.getText();

                try {
                    zkw.ZKWriter(clientName, UserConsole.this);
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

        userComboBox.addFocusListener(new FocusAdapter() {
            @Override
            public void focusGained(FocusEvent e) {
                super.focusGained(e);
                userComboBox.removeAllItems();

                List <String> childrenOnlineUsers = null;
                try {
                    childrenOnlineUsers = zkw.zoo.getChildren("/System/Online", false);
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
/*
                for(int i = 0; i < childrenOnlineUsers.size(); i++)
                    System.out.println(childrenOnlineUsers.get(i));  //To show on console
                    userComboBox.addItem(childrenOnlineUsers); // To add in the ComboBox
                    userComboBox.addItem(childrenOnlineUsers);
                return;*/

                userComboBox.setModel(new DefaultComboBoxModel(childrenOnlineUsers.toArray()));

            }
        });

    }


    public void addMessage(List<String> messages){


            ConsoleReading.setVisible(true);
            chatlabel.setVisible(true);

            for (String received : messages) {

                //ConsoleReading.setText(received);
                ConsoleReading.append(received);
                ConsoleReading.append("\n");
            }


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

/*
    private void createUIComponents() {
        TODO: place custom component creation code here
    }*/
}
