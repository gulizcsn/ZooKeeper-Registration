package es.upm.master.zookeeper.kafkaCode;

import es.upm.master.zookeeper.kafkaCode.kafkaUConsole;
import es.upm.master.zookeeper.kafkaCode.ZKWriter;
import es.upm.master.zookeeper.kafkaCode.kafkaUConsole;
import org.apache.zookeeper.KeeperException;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class kafkaUConsole {
    private JTextField kafkaUsername;
    private JButton kafkaRegister;
    private JPanel formulario;
    private JButton kafkaSend;
    private JComboBox kafkaComboBox;
    private JTextField kafkaTxtField;
    private JTextArea kafkaTextArea;
    private JButton kafkaRead;
    private JTextArea kafkaChat;
    private JLabel kafkaMessageToLabel;
    private JLabel kafkaMessageLabel;
    private JLabel kafkaUsernameLabel;

    public kafkaUConsole() {
        ZKWriter zkw = new ZKWriter();

        formulario.setVisible(true);
        kafkaUsername.setVisible(true);
        kafkaUsernameLabel.setVisible(true);
        kafkaRegister.setVisible(true);
        kafkaSend.setVisible(false);
        kafkaComboBox.setVisible(false);
        kafkaTxtField.setVisible(false);
        kafkaTextArea.setVisible(false);
        kafkaRead.setVisible(false);
        kafkaChat.setVisible(false);
        kafkaMessageToLabel.setVisible(false);
        kafkaMessageLabel.setVisible(false);

        kafkaRegister.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                String clientName = kafkaUsername.getText();

                formulario.setVisible(true);
                kafkaUsername.setEditable(false);
                kafkaUsernameLabel.setVisible(false);
                kafkaRegister.setVisible(false);
                kafkaSend.setVisible(true);
                kafkaComboBox.setVisible(true);
                kafkaTxtField.setVisible(false);
                kafkaTextArea.setVisible(true);
                kafkaRead.setVisible(true);
                kafkaChat.setVisible(true);
                kafkaMessageToLabel.setVisible(true);
                kafkaMessageLabel.setVisible(true);


                try {
                    zkw.ZKWriter(clientName, kafkaUConsole.this);
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

            }
        });
        kafkaSend.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {

                //String receiver = kafkaTxtField.getText();
                String receiver = kafkaComboBox.getSelectedItem().toString();
                String message = kafkaTextArea.getText();
                try {
                    zkw.send(receiver, message);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

            }
        });


        kafkaComboBox.addFocusListener(new FocusAdapter() {
            @Override
            public void focusGained(FocusEvent focusEvent) {

                super.focusGained(focusEvent);
                kafkaComboBox.removeAllItems();

                List<String> childrenOnlineUsers = null;
                try {
                    childrenOnlineUsers = zkw.zoo.getChildren("/System/Online", false);
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                kafkaComboBox.setModel(new DefaultComboBoxModel(childrenOnlineUsers.toArray()));


            }
        });
        kafkaRead.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {

                try {
                    zkw.read();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

            }
        });

        kafkaComboBox.addFocusListener(new FocusAdapter() {
            @Override
            public void focusGained(FocusEvent e) {
                super.focusGained(e);

                List <String> childrenOnlineUsers = null;
                try {
                    childrenOnlineUsers = zkw.zoo.getChildren("/System/Online", false);
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                kafkaComboBox.setModel(new DefaultComboBoxModel(childrenOnlineUsers.toArray()));
            }
        });
    }
        public void addMessage(List<String> messages){

            for (String received : messages) {

                //ConsoleReading.setText(received);
                kafkaChat.append(received);
                kafkaChat.append("\n");
            }


        }
        //TODO: add Go offline and quit. 


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        JFrame frames = new JFrame("ZooApp");
        frames.setPreferredSize(new Dimension(500,350));
        frames.setLocation(500,250);
        frames.setContentPane(new kafkaUConsole().formulario);
        frames.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frames.pack();
        frames.setVisible(true);

    }

}
